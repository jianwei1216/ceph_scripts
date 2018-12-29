#!/usr/bin/python

import rados
import rbd
import json

class CephOP(object):
    def __init__(self, username=None, conffile=None):
        self.__username = username
        self.__conffile = conffile
        self.__cluster = rados.Rados(conffile=self.__conffile)
        self.__cluster.connect()

    def __exit__(self):
        self.__cluster.shutdown()

    def _mon_command(self, cmd, *args):
        ret, buf, out =  self.__cluster.mon_command(json.dumps(cmd), '')
        print cmd, ':---ret------------\n', ret
        print cmd, ':---buf------------\n', buf
        print cmd, ':---out------------\n', out

    def _mgr_command(self, cmd, *args):
        ret, buf, out =  self.__cluster.mgr_command(json.dumps(cmd), '')
        print cmd, ':---ret------------\n', ret
        print cmd, ':---buf------------\n', buf
        print cmd, ':---out------------\n', out

    def ceph_status(self):
        cmd = {'prefix':'status'}
        self._mon_command(cmd)

    def ceph_osd_dump(self):
        cmd = {'prefix':'osd dump'}
        self._mon_command(cmd)

    def ceph_health_detail(self):
        cmd = {'prefix':'health', 'detail':'detail'}
        self._mon_command(cmd)

    def ceph_osd_pool_stats(self):
        cmd = {'prefix':'osd pool stats'}
        #self._mgr_command(cmd)
        self._mon_command(cmd)

    def ceph_osd_pool_ls(self):
        cmd = {'prefix':'osd pool ls'}
        self._mon_command(cmd)

    def ceph_osd_tree(self):
        cmd = {'prefix':'osd tree'}
        self._mon_command(cmd)

    def ceph_pg_dump_osds(self):
        cmd = {'prefix':'pg dump', 'dumpcontents':['osds', 'pools']}
        self._mon_command(cmd)

    def ceph_pg_dump_pools(self):
        cmd = {'prefix':'pg dump', 'dumpcontents':'osds'}
        self._mon_command(cmd)

    def ceph_osd_lspools(self):
        cmd = {'prefix':'osd lspools'}
        self._mon_command(cmd)

    def ceph_osd_pool_ls_detail(self):
        cmd = {'prefix':'osd pool ls', 'detail':'detail', 'format':'json'}
        self._mon_command(cmd)

    def ceph_osd_pool_get_erasure_code_profile(self):
        cmd = {'prefix':'osd pool get', 'var':'erasure_code_profile', 'pool':'pool1'}
        self._mon_command(cmd)

    def ceph_osd_erasure_code_profile_get(self):
        cmd = {'prefix':'osd erasure-code-profile get', 'name':'erasure-code-profile'}
        self._mon_command(cmd)

    def ceph_osd_crush_rule_dump(self):
        cmd = {'prefix':'osd crush rule dump', 'format':'json'}
        self._mon_command(cmd)

    def ceph_df(self):
        cmd = {'prefix':'df'}
        self._mon_command(cmd)

if __name__ == '__main__':
    cluster = CephOP('admin', '/etc/ceph/ceph.conf')

    #cluster.ceph_status()
    #cluster.ceph_osd_dump()
    #cluster.ceph_health_detail()
    #cluster.ceph_osd_pool_stats()
    #cluster.ceph_osd_pool_ls()
    #cluster.ceph_osd_tree()
    cluster.ceph_pg_dump_osds()
    #cluster.ceph_pg_dump_pools()
    #cluster.ceph_osd_lspools()
    #cluster.ceph_df()
    #cluster.ceph_osd_pool_ls_detail()
    #cluster.ceph_osd_pool_get_erasure_code_profile()
    #cluster.ceph_osd_erasure_code_profile_get()
    #cluster.ceph_osd_crush_rule_dump()
    #cluster.ceph_df()
    #cluster.ceph_pool_pg_status()
