#!/usr/bin/python

import rados
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

def main():
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

if __name__ == '__main__':
    main()
