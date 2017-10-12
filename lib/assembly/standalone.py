"""
Run a standalone ARAST job
"""

import pprint
import uuid
import copy
import errno
import glob
import logging
import sys
import json
import requests
import os
import shutil
import time
import datetime
import socket
import multiprocessing
import re
import threading
import subprocess
from multiprocessing import current_process as proc
from traceback import format_tb, format_exc

import assembly as asm
import metadata as meta
import asmtypes
import wasp
import recipes
import utils
from assembly import ignored
from job import ArastJob
from kbase import typespec_to_assembly_data as kb_to_asm
from plugins import ModuleManager

logger = logging.getLogger(__name__)

class ArastStandalone:
    def __init__(self, threads, datapath, binpath, modulebin):

        self.threads = threads
        self.binpath = binpath
        self.modulebin = modulebin
        self.pmanager = ModuleManager(threads, None, None, None, binpath, modulebin)

        self.datapath = datapath

    def compute(self, jobpath, input_description):

        try:
            os.makedirs(jobpath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        pipelines = input_description['pipelines']
        recipe = input_description['recipe']
        wasp_in = input_description['wasp_in']

        ### Create job log
        self.out_report_name = '{}/{}_report.txt'.format(jobpath, str(input_description['job_id']))
        self.out_report = open(self.out_report_name, 'w')

        job_id = input_description['job_id']

        # create job data (ArastJob object)

        #
        # input_description is dictionary containing three
        # input sets: reads, reference, and contigs.
        # Each contains a list of fileinfo objects.
        #
        # It also contains fields user, containing the end system's username,
        # and job_id, a job_id allocated by the end system.
        # 

        uid = str(uuid.uuid4())

        #
        # We need to populate the files list in each of the filesets.
        #
        print input_description
        for sub in ['reads', 'reference', 'contigs']:
            l = input_description[sub]
            for fs in l:
                print sub, fs
                fs['files'] = []
                for x in fs['fileinfos']:
                    fs['files'].append(x['local_file'])
        print input_description

        job_data = ArastJob({'job_id' : job_id,
                             'uid': uid,
                             'user' : input_description['user'],
                             'reads' : input_description['reads'],
                             'logfiles': [],
                             'reference': input_description['reference'],
                             'contigs': input_description['contigs'],
                             'initial_reads': list(input_description['reads']),
                             'raw_reads': copy.deepcopy(input_description['reads']),
                             'params' : [],
                             'exceptions' : [],
                             'pipeline_data' : {},
                             'out_report': self.out_report,
                             'datapath': self.datapath
                             })
                             
        
        status = ''
        logger.debug('job_data = {}'.format(job_data))

        self.start_time = time.time()

        #### Parse pipeline to wasp exp
        reload(recipes)
        if recipe:
            try: wasp_exp = recipes.get(recipe[0], job_id)
            except AttributeError: raise Exception('"{}" recipe not found.'.format(recipe[0]))
        elif wasp_in:
            wasp_exp = wasp_in[0]
        elif not pipelines:
            wasp_exp = recipes.get('auto', job_id)
        elif pipelines:
            ## Legacy client
            if pipelines[0] == 'auto':
                wasp_exp = recipes.get('auto', job_id)
            ##########
            else:
                if type(pipelines[0]) is not list: # --assemblers
                    pipelines = [pipelines]
                all_pipes = []
                for p in pipelines:
                    all_pipes += self.pmanager.parse_input(p)
                logger.debug("pipelines = {}".format(all_pipes))
                wasp_exp = wasp.pipelines_to_exp(all_pipes, params['job_id'])
        else:
            raise asmtypes.ArastClientRequestError('Malformed job request.')
        logger.debug('Wasp Expression: {}'.format(wasp_exp))
        w_engine = wasp.WaspEngine(self.pmanager, job_data)

        ###### Run Job
        try:
            w_engine.run_expression(wasp_exp, job_data)
            ###### Upload all result files and place them into appropriate tags

            print "Done - job data: " , pprint.pformat(job_data)
            # uploaded_fsets = job_data.upload_results(url, token)

            # Format report
            new_report = open('{}.tmp'.format(self.out_report_name), 'w')

            ### Log errors
            if len(job_data['errors']) > 0:
                new_report.write('PIPELINE ERRORS\n')
                for i,e in enumerate(job_data['errors']):
                    new_report.write('{}: {}\n'.format(i, e))
            try: ## Get Quast output
                quast_report = job_data['wasp_chain'].find_module('quast')['data'].find_type('report')[0].files[0]
                with open(quast_report) as q:
                    new_report.write(q.read())
            except:
                new_report.write('No Summary File Generated!\n\n\n')
            self.out_report.close()
            with open(self.out_report_name) as old:
                new_report.write(old.read())

            for log in job_data['logfiles']:
                new_report.write('\n{1} {0} {1}\n'.format(os.path.basename(log), '='*20))
                with open(log) as l:
                    new_report.write(l.read())

            ### Log tracebacks
            if len(job_data['tracebacks']) > 0:
                new_report.write('EXCEPTION TRACEBACKS\n')
                for i,e in enumerate(job_data['tracebacks']):
                    new_report.write('{}: {}\n'.format(i, e))

            new_report.close()
            os.remove(self.out_report_name)
            shutil.move(new_report.name, self.out_report_name)
            # res = self.upload(url, user, token, self.out_report_name)
            print "Would upload ", self.out_report_name
            # report_info = asmtypes.FileInfo(self.out_report_name, shock_url=url, shock_id=res['data']['id'])

            status = 'Complete with errors' if job_data.get('errors') else 'Complete'

            ## Make compatible with JSON dumps()
            del job_data['out_report']
            del job_data['initial_reads']
            del job_data['raw_reads']
            #
            # Write this somewhere
            # self.metadata.update_job(uid, 'data', job_data)
            # self.metadata.update_job(uid, 'result_data', uploaded_fsets)

            sys.stdout.flush()
            touch(os.path.join(jobpath, "_DONE_"))
            logger.info('============== JOB COMPLETE ===============')

        except asmtypes.ArastUserInterrupt:
            status = 'Terminated by user'
            sys.stdout.flush()
            touch(os.path.join(jobpath, "_CANCELLED__"))
            logger.info('============== JOB KILLED ===============')



### Helper functions ###
def touch(path):
    logger.debug("touch {}".format(path))
    now = time.time()
    try:
        os.utime(path, (now, now))
    except os.error:
        pdir = os.path.dirname(path)
        if len(pdir) > 0 and not os.path.exists(pdir):
            os.makedirs(pdir)
        open(path, "a").close()
        os.utime(path, (now, now))

def is_filename(word):
    return word.find('.') != -1 and word.find('=') == -1

def is_dir_busy(d):
    busy = False
    if not os.path.exists(d):
        logger.info("GC: directory not longer exists: {}".format(d))
        return False
    if re.search(r'raw/*$', d): # data path: check if no job directories exist
        fname = os.path.join(d, "_READY_")
        dirs = glob.glob(d + '/../*/')
        logger.debug("GC: data directory {} contains {} jobs".format(d, len(dirs)-1))
        busy = len(dirs) > 1 or not os.path.exists(fname)
    else:                       # job path
        fname1 = os.path.join(d, '_DONE_')
        fname2 = os.path.join(d, '_CANCELLED_')
        busy = not (os.path.exists(fname1) or os.path.exists(fname2))
    if busy:
        logger.debug("GC: directory is busy: {}".format(d))
    return busy

def free_space_in_path(path):
    s = os.statvfs(path)
    free_space = float(s.f_bsize * s.f_bavail / (10**9))
    logger.debug("Free space in {}: {} GB".format(path, free_space))
    return free_space

