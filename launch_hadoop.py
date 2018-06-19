# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# File Name: launch_hadoop.py
# Author: Zhimin Xu
# Mail: xuzhimin@baidu.com
# Created Time: Mon 07 May 2018 08:38:46 PM CST
# -------------------------------------------------------------------------------

import sys, os, shutil
import commands
import logging
import time
import re
from ConfigParser import ConfigParser
import subprocess
import shlex



def check_repeat_section(configfile):
    sectioninfo = {}
    attrinfo = {}
    for no, line in enumerate(open(configfile).readlines()):
        line = line.strip()
        _sec = re.match('\[.+\]', line)
        if _sec is not None:
            _sec = _sec.group()
            section = _sec[1:-1]
            if sectioninfo.has_key(section):
                logging.error('error! line:%d %s has repeat section "%s" with line:%d %s.' %
                        (no+1, line, section, sectioninfo[section][0], sectioninfo[section][1]) )
                return False
            else:
                sectioninfo[section] = [no, line]
            attrinfo = {}
        
        if not line.startswith('#') and ('=' in line):
            attr = line.split('=')[0]
            if attrinfo.has_key(attr):
                logging.error('error! line:%d %s has repeat attr "%s" in section "%s" with line:%d %s.' %
                        (no+1, line, attr, section, attrinfo[attr][0], attrinfo[attr][1]) )
                return False
            else:
                attrinfo[attr] = [no, line]
            if ' ;' in line:
                logging.error('*** dont let Space before ";", it may be wrong because ConfigParser lib, \
                        or instand with "," ***\nline:%d %s' % (no+1, line) )
                return False
    return True


def get_hadoop_client(config, section):
    if config.has_option(section, 'HADOOP_HOME'):
        HadoopHome = config.get(section, 'HADOOP_HOME')
    else:
        HadoopHome = '/home/vis/xuzhimin/eval_folder/hadoop-client/hadoop/'
    if config.has_option(section, 'hadoop_conf'):
        hadoop_conf = config.get(section, 'hadoop_conf')
    else:
        hadoop_conf = 'hadoop-site.xml.nmg'
    shutil.copy(os.path.join(HadoopHome, 'conf', hadoop_conf), os.path.join(HadoopHome, 'conf/hadoop-site.xml'))

    HadoopBin = os.path.join(HadoopHome, 'bin/hadoop')
    return HadoopBin


# read streaming type
def get_io_format(config, section):
    streamingtype = config.get(section, 'streamingtype') if config.has_option(section, 'streamingtype') else 'bistreaming'
    if streamingtype == 'bistreaming':
        inputformat = "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"
        outputformat = "org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat"
    elif streamingtype == 'streaming':
        inputformat = "org.apache.hadoop.mapred.TextInputFormat"
        outputformat = "org.apache.hadoop.mapred.TextOutputFormat"
    elif streamingtype == 'ustreaming':
        assert config.has_option(section, 'inputformat'), 'when use ustreaming must specify inputformat: txt /  binary'
        inputformatDict = {'txt':"org.apache.hadoop.mapred.TextInputFormat", 'binary':"org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"}
        inputformat = inputformatDict[config.get(section, 'inputformat')]
        assert config.has_option(section, 'outputformat'), 'when use ustreaming must specify outputformat: txt /  binary'
        outputformatDict = {'txt':"org.apache.hadoop.mapred.TextOutputFormat", 'binary':"org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat"}
        outputformat = outputformatDict[config.get(section, 'outputformat')]
    return streamingtype, inputformat, outputformat


def get_cache_local(config, section):
    archives_str = config.get(section, 'cacheArchive').strip() if config.has_option(section, 'cacheArchive') else ''
    cachefiles_str = config.get(section, 'cacheFile').strip() if config.has_option(section, 'cacheFile') else ''
    localfiles_str = config.get(section, 'files').strip() if config.has_option(section, 'files') else ''
    # cacheArchive
    if archives_str != '':
        if (archives_str[0] == archives_str[-1]) and archives_str.startswith(("'", '"')):
            archives_str = archives_str[1:-1]
        cacheArchive = filter(lambda x: x.strip() != '', archives_str.split(','))
    else:
        cacheArchive = []
    # cacheFile
    if cachefiles_str != '':
        if (cachefiles_str[0] == cachefiles_str[-1]) and cachefiles_str.startswith(("'", '"')):
            cachefiles_str = cachefiles_str[1:-1]
        cacheFile = filter(lambda x: x.strip() != '', cachefiles_str.split(','))
    else:
        cacheFile = []
    # localFile
    if localfiles_str != '':
        if (localfiles_str[0] == localfiles_str[-1]) and localfiles_str.startswith(("'", '"')):
            localfiles_str = localfiles_str[1:-1]
        localFile = filter(lambda x: x.strip() != '', localfiles_str.split(','))
    else:
        localFile = []
    return cacheArchive, cacheFile, localFile

    


def get_other_args(config, section):
    if config.has_option(section, 'append_args'):
        append_args_str = config.get(section, 'append_args').strip()
        if (append_args_str[0] == append_args_str[-1]) and append_args_str.startswith(("'", '"')):
            append_args_str = append_args_str[1:-1]
        append_args = [x.strip() for x in append_args_str.split(',')]
    else:
        append_args = []
    return append_args


# create files,  logfile, failedlogfile, _tempfile
def create_log_file(config, section):
    LOG_PATH = '/home/vis/xuzhimin/eval_folder/run_hadoop/hadooptask_log/'
    curdate = time.strftime("%Y-%m")
    logfile = os.path.join(LOG_PATH, 'hadooptask_%s.log' % curdate)
    if not os.path.exists(logfile):
        open(logfile, 'wb').write(curdate + '\n\n')
    failedlogfile = os.path.join(LOG_PATH, 'failed_hadooptask_%s.log' % curdate)
    if not os.path.exists(failedlogfile):
        open(failedlogfile, 'wb').write(curdate + '\n\n')
    curtime = time.strftime("%Y-%m-%d_%H:%M:%S")
    tempfile = os.path.join(LOG_PATH, '_temp_%s_%s.log'% (curtime, section) ) 
    return logfile, failedlogfile, tempfile


def launch(configfile, section):
    assert check_repeat_section(configfile), 'please check config %s'%configfile
    config = ConfigParser()
    config.readfp(open(configfile,'r'))

    HadoopBin = get_hadoop_client(config, section) 

    job_name = config.get(section, 'job_name') if config.has_option(section, 'job_name') else 'xzm_%s' % section
    priority = config.get(section, 'priority') if config.has_option(section, 'priority') else 'VERY_HIGH'

    # read map and reduce, cmd and num
    map_num = config.getint(section, 'map_num') if config.has_option(section, 'map_num') else 5000
    reduce_num = config.getint(section, 'reduce_num') if config.has_option(section, 'reduce_num') else 2000
    map_capacity = map_num
    reduce_capacity = reduce_num

    mapper = config.get(section, 'mapper') 
    reducer = config.get(section, 'reducer') if reduce_num != 0 else "cat"
    
    input_folder = config.get(section, 'input_folder') 
    output_folder = config.get(section, 'output_folder') 

    streamingtype, inputformat, outputformat = get_io_format(config, section)
    # cacheArchive, cacheFile, file
    cacheArchive, cacheFile, localFile = get_cache_local(config, section)

    # some other args
    append_args = get_other_args(config, section)

    section_items = config.items(section)

    #########  hadoop command
    hadoop_command = [ '%s %s' % (HadoopBin, streamingtype) ]
    for _file in cacheFile:
        hadoop_command.append('-files %s' % _file.strip())
    hadoop_command.append('-D mapred.job.name=%s' % job_name )
    hadoop_command.append('-D mapred.job.priority=%s' % priority )
    hadoop_command.append('-D mapred.map.tasks=%d' % map_num )
    hadoop_command.append('-D mapred.reduce.tasks=%d' % reduce_num )
    hadoop_command.append('-D mapred.job.map.capacity=%d' % map_capacity )
    hadoop_command.append('-D mapred.job.reduce.capacity=%d' % reduce_capacity )
    hadoop_command.append('-D mapred.map.over.capacity.allowed=true')
    hadoop_command.append('-D mapred.reduce.over.capacity.allowed=true')
    hadoop_command.append('-D mapred.max.map.failures.percent=3')
    hadoop_command.append('-D mapred.max.reduce.failures.percent=3')
    for _args in append_args:
        hadoop_command.append(_args)
    hadoop_command.append('-inputformat %s' % inputformat )
    hadoop_command.append('-outputformat %s' % outputformat )
    for _archive in cacheArchive:
        hadoop_command.append('-cacheArchive %s' % _archive.strip())
    for _file in localFile:
        hadoop_command.append('-file %s' % _file.strip())
    hadoop_command.append('-mapper %s' % mapper )
    hadoop_command.append('-reducer %s' % reducer )
    hadoop_command.append('-input %s' % input_folder )
    hadoop_command.append('-output %s' % output_folder )

    hadoop_command_str = '\n'.join(hadoop_command)
    logging.info('\n' + hadoop_command_str)     

    ## log file
    successlogfile, failedlogfile, tempfile = create_log_file(config, section)

    fo = open(tempfile, 'wb')
    fo.write('\n\n' + '=' * 100 + '\n')
    fo.write(time.strftime("%Y-%m-%d_%H:%M:%S") + '\t\tlaunch %s\n'%sys.argv[1:] + hadoop_command_str + '\n--------\n')
    fo.write('%s\n' % str(section_items) )

    print >> sys.stderr, '----\n%s\n----' % str( shlex.split(hadoop_command_str) )
    killcmdstr = ''
    task_successful = 1
    try:
        # shell=False, just the first one is the execute command, other params
        proc = subprocess.Popen( shlex.split(hadoop_command_str), stdout=subprocess.PIPE, stderr = subprocess.STDOUT, shell=False )
        for line in iter(proc.stdout.readline, ''):
            fo.write(line)
            fo.flush()
            sys.stdout.write(line)
            if '-kill' in line:
                killcmdstr = line
            if 'Job not Successful' in line:
                task_successful = 0
        proc.stdout.close()
        if proc.poll() is not None:  
            proc.wait()  
        fo.close()
    except KeyboardInterrupt:
        with open(failedlogfile, 'a') as outfile:
            returncode = subprocess.call(["cat", tempfile], stdout=outfile, stderr = outfile)
            outfil.write('KeyboardInterrupt!')
        killcmdstr = killcmdstr.split('mapred.JobClient:')[-1]
        logging.info('\n***** KeyboardInterrupt! ******\n if want kill this job, run:\n' + killcmdstr)
    else:
        if task_successful:
            logfile = successlogfile
        else:
            logfile = failedlogfile
        with open(logfile, 'a') as outfile:
            returncode = subprocess.call(["cat", tempfile], stdout=outfile, stderr = outfile)
    finally:
        os.remove(tempfile)




if __name__ == '__main__':
    LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL,
            format='%(levelname)s:[%(lineno)d]%(name)s:%(funcName)s->%(message)s',  #logger.BASIC_FORMAT,
            datefmt='%a, %d %b %Y %H:%M:%S')
    if len(sys.argv) != 3:
        print >> sys.stderr, 'usage:'
        print >> sys.stderr, 'python launch_hadoop.py  configfile  section'
    else:
        print >> sys.stderr, 'launch ', sys.argv[1:] 
        launch(*sys.argv[1:])

