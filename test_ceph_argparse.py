#!/usr/bin/python

import rados
import json
import time
from ceph_argparse import run_in_thread, json_command
from ceph_argparse import parse_json_funcsigs, send_command, parse_funcsig
from ceph_argparse import validate_command

cluster_handle = None

def test_json_cmd(cluster_handle, prefixstr, args=None, inbuf=None):
    ret, out, err = json_command(cluster_handle, prefix=prefixstr, argdict=args)
    if prefixstr != 'get_command_descriptions':
        print 'DEBUG: [prefix=%s, args=%s], ret=\n%s' % (prefixstr, args, ret)
        print 'DEBUG: [prefix=%s, args=%s], out=\n%s' % (prefixstr, args, out)
        print 'DEBUG: [prefix=%s, args=%s], err=\n%s' % (prefixstr, args, err)
    return out

def test_get_sigdict(cluster_handle):
    ret, outbuf, err = json_command(cluster_handle, prefix='get_command_descriptions')
    sigdict = parse_json_funcsigs(outbuf.decode('utf-8'), 'cli')
    return sigdict

def test_json_cmd2(cluster_handle, args_dict, fmt='json'):
    args_dict['format'] = fmt
    ret, out, err = json_command(cluster_handle, argdict=args_dict)
    if ret:
        print 'DEBUG: args=%s, ret=\n%s' % (args_dict, ret)
    if out:
        print 'DEBUG: args=%s, out=\n%s' % (args_dict, out)
    if err:
        print 'DEBUG: args=%s, err=\n%s' % (args_dict, err)

def test_json_cmd3(cluster_handle, sigdict, argstr):
    print 'DEBUG: sigdict=%s, argstr=%s' % (type(sigdict), type(argstr))
    arg_list = argstr.split(' ')
    valid_dict = validate_command(sigdict, arg_list, False)
    test_json_cmd2(cluster_handle, valid_dict)

def test_get_valid_dict(sigdict, arg_list):
    valid_dict = validate_command(sigdict, arg_list, False)
    return valid_dict

def test1():
    with rados.Rados(rados_id='admin', conffile='/etc/ceph/ceph.conf') as cluster_handle:
        #print cluster_handle.get_fsid()
        #test_json_cmd(cluster_handle, prefixstr='status')
        #test_json_cmd(cluster_handle, prefixstr='pg dump',
        #              args={'dumpcontents':['osds', 'pools']})
        #test_send_command(cluster_handle, 'status')

        sigdict = test_get_sigdict(cluster_handle)

        argstr = ('osd erasure-code-profile set ec_21_p123_profile k=2 m=1 '
                  'ruleset-failure-domain=host root=hdd-default '
                  'crush-root=hdd-default ruleset-root=hdd-default')
        test_json_cmd3(cluster_handle, sigdict, argstr)

        argstr = ('osd crush rule create-erasure '
                  'ec_21_p123_rule ec_21_p123_profile')
        test_json_cmd3(cluster_handle, sigdict, argstr)

        argstr = ('osd pool create p123 64 64 '
                  'erasure ec_21_p123_profile ec_21_p123_rule')
        test_json_cmd3(cluster_handle, sigdict, argstr)

class TestCephClusterArgParse(object):
    def __init__(self, username=None, conffile=None, need_connect=True):
        self.__user_name = username
        self.__conf_file = conffile
        self.__cluster_handle = rados.Rados(rados_id=self.__user_name,
                                            conffile=self.__conf_file)
        if need_connect:
            self.connect()

    def connect(self):
        self.__cluster_handle.connect(1)
        self.__sig_dict = self.__get_sigdict()

    def disconnect(self):
        self.__cluster_handle.shutdown()

    def __del__(self):
        self.disconnect()

    def __get_sigdict(self):
        ret, outbuf, err = json_command(self.__cluster_handle,
                                        prefix='get_command_descriptions')
        if ret != 0:
            print 'ERROR: ret=%s, outbuf=%s, err=%s' % (ret, outbuf, err)
        print '---debug--', ret
        sigdict = parse_json_funcsigs(outbuf.decode('utf-8'), 'cli')
        #print ('---debug---type(outbuf)=%s, type(sigdict)=%s' %
        #      (type(outbuf), type(sigdict)))
        return sigdict

    def __get_valid_dict(self, args_str, verbose=False):
        args_list = args_str.split(' ')
        args_dict = validate_command(self.__sig_dict, args_list, verbose)
        return args_dict

    def __run_json_command(self, args_str, fmt='json'):
        args_dict = self.__get_valid_dict(args_str)
        args_dict['format'] = fmt
        #ret, out, err = self.__cluster_handle.mon_command(json.dumps(args_dict), '')
        ret, out, err = json_command(self.__cluster_handle, argdict=args_dict)
        if ret:
            print 'DEBUG: args=%s, ret=\n%s' % (args_dict, ret)
        if out:
            print 'DEBUG: args=%s, out=\n%s' % (args_dict, out)
        if err:
            print 'DEBUG: args=%s, err=\n%s' % (args_dict, err)
        return ret, out, err

    def create_pool(self):
        args_str = ('osd erasure-code-profile set ec_21_p123_profile k=2 m=1 '
                    'ruleset-failure-domain=host root=hdd-default '
                    'crush-root=hdd-default ruleset-root=hdd-default')
        ret, out, err = self.__run_json_command(args_str)
        args_str = ('osd crush rule create-erasure '
                    'ec_21_p123_rule ec_21_p123_profile')
        ret, out, err = self.__run_json_command(args_str)
        args_str = ('osd pool create p123 64 64 '
                    'erasure ec_21_p123_profile ec_21_p123_rule')
        ret, out, err = self.__run_json_command(args_str)

    def delete_pool(self):
        args_str = 'osd pool delete p123 p123 --yes-i-really-really-mean-it'
        ret, out, err = self.__run_json_command(args_str)

    def list_pools(self):
        args_str = 'osd lspools'
        ret, out, err = self.__run_json_command(args_str)

    def get_rados_status(self):
        self.__cluster_handle.require_state("connected")
        print '---debug---self.__cluster_handle.state=', self.__cluster_handle.state
        #self.__cluster_handle.require_state("shutdown")
        #self.__cluster_handle.require_state("configuring")
        #self.__cluster_handle.require_state("disconnect")

def test2():
    cluster = TestCephClusterArgParse('admin', '/etc/ceph/ceph.conf')
    while True:
        cluster.list_pools()
        cluster.get_rados_status()
        time.sleep(0.5)
    #cluster.create_pool()
    #cluster.list_pools()
    #cluster.delete_pool()
    #cluster.list_pools()

    print '------------------------------'

def main():
    test2()

if __name__ == '__main__':
    main()
