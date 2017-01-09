#!/usr/bin/env python
'''
  a stupidly basic pipeline for testing
'''

import argparse
import os
import sys
import datetime
import subprocess

def log(sev, msg):
    when = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    sys.stderr.write('{0}: {1} {2}\n'.format(when, sev, msg))

def run_command(line, command, config):
    configured_command = command
    for key in config:
       configured_command = configured_command.replace('{{{}}}'.format(key), config[key]) # {KEY} -> keyvalue
    log('INFO', 'running line {}: {} from template {}...'.format(line, configured_command, command))

    result = os.system(configured_command)
    if result != 0:
        log('ERROR', 'running line {}: {}: FAILED: {}'.format(line, configured_command, result))
        return False

    #try:
    #    subprocess.check_output(configured_command, shell=True)
    #except subprocess.CalledProcessError as e:
    #    log('ERROR', 'running line {}: {}: FAILED: returncode: {}. output: {}'.format(line, configured_command, e.returncode, e.output))
    #    return False
        
    log('INFO', 'running line {}: {}: done'.format(line, configured_command))
    return True

def run_pipeline(config_fh, commands, resume=0):
    log('INFO', 'xpipe is starting')
    # read config
    config = {}
    for line in config_fh:
        if line.startswith('#') or len(line.strip()) == 0:
            continue
        fields = line.strip('\n').split('=')
        config[fields[0]] = fields[1]
    log('INFO', 'Loaded {} configuration settings'.format(len(config)))

    # run commands
    ok = True
    line = 0
    for line, command in enumerate(commands):
        command = command.strip()
        if command.startswith('#') or len(command) == 0:
            log('INFO', command) # comment
            continue
        if line + 1 < resume:
            log('INFO', 'skipping line {}: {}'.format(line + 1, command)) 
            continue
        if len(command) == 0:
            continue
        if not run_command(line + 1, command, config):
            ok = False # problem
            break
    
    log('INFO', 'xpipe is finished')
    if not ok:
        log('ERROR', 'xpipe encountered an error on line {}'.format(line + 1))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extremely simple pipeline')
    parser.add_argument('--config', required=True, help='configuration options')
    parser.add_argument('--resume', required=False, type=int, default=0, help='line number in file to start from')
    args = parser.parse_args()
    # now do each stage...
    run_pipeline(config_fh=open(args.config, 'r'), commands=sys.stdin, resume=args.resume)
