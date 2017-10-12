#
# Perform a standalone run of a job.
#

import json
import sys
import standalone
import argparse
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(asctime)s %(levelname)s %(process)d %(name)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)

def main():

    parser = argparse.ArgumentParser(description = "Standalone ARAST job runner")
    parser.add_argument('-d', '--data-path', required = True)
    parser.add_argument('-b', '--bin-path', required = True)
    parser.add_argument('-j', '--job-path', required = True)
    parser.add_argument('-m', '--module-bin', required = True)
    parser.add_argument('-n', '--num-threads', default = 4)
    parser.add_argument('input_description')
    
    args = parser.parse_args()
    print args

    fh = file(args.input_description)
    input_description = json.load(fh)
    fh.close()

    sa = standalone.ArastStandalone(args.num_threads, os.path.abspath(args.data_path),
                                    os.path.abspath(args.bin_path), os.path.abspath(args.module_bin))

    sa.compute(args.job_path, input_description)

if __name__ == '__main__':
    main()
