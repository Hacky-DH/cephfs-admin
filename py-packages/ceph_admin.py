#!/bin/env python
'''
ceph admin tool
changlog
use env and add root prefix
'''

from __future__ import print_function
import sys
import os
import json
import argparse

import ceph_admin_interface as adminI

home_dir=os.getenv('CEPH_ADMIN_HOME', '.')
default_log_conf = os.path.join(home_dir, 'conf', 'logging.conf')
default_admin_conf = os.path.join(home_dir, 'conf', 'admin.info')

def adduser_handler(**kwargs):
    try:
        key = adminI.adduser(**kwargs)
        return 0
    except Exception as e:
        print('add user error:', e)
        return 1

def updateuser_handler(**kwargs):
    try:
        adminI.updateuser(**kwargs)
        return 0
    except Exception as e:
        print('update user error:', e)
        return 1

def deluser_handler(**kwargs):
    try:
        adminI.deluser(**kwargs)
        return 0
    except Exception as e:
        print('delete user error:', e)
        return 1

def lsuser_handler(**kwargs):
    try:
        users = adminI.lsuser(**kwargs)
        for u in users:
            print(u)
        return 0
    except Exception as e:
        print('list user error:', e)
        return 1

def getuser_handler(**kwargs):
    try:
        kwargs['showpath'] = True
        key, usage = adminI.getuser(**kwargs)
        user = kwargs.get('user')
        print('user {0}\n\tkey: {1}\n\tpath prefix: {2}\nusage:'
            .format(user, key, adminI.root_prefix))
        for used, quota, path in usage:
            print('\tpath: ' + path)
            retio = '%.2f' % (float(used)/float(quota)) if quota != '0' else 0
            print('\t\tused: {0}\n\t\t%used: {1}%\n\t\tquota: {2}'
                .format(adminI.format_bytes(used), retio, adminI.format_bytes(quota)))
        return 0
    except Exception as e:
        print('get user error:', e)
        return 1

def exportuser_handler(**kwargs):
    try:
        info = adminI.exportuser(**kwargs)
        print(info)
        f = kwargs.get('infofile')
        if f:
            f.write(info)
            f.close()
        return 0
    except Exception as e:
        print('export user error:', e)
        return 1

def show_handler(**kwargs):
    try:
        cfg, info = adminI.show_info(**kwargs)
        print('config file: {}\n{}'.format(cfg, info))
        return 0
    except Exception as e:
        print('show error:', e)
        return 1

def parse_cmdargs(args=None):
    parser = argparse.ArgumentParser(description='ceph admin tool')
    parser.add_argument('-v', '--version', action="store_true", help="display version")
    parser.add_argument('-vv', '--verbose', action="store_true", help="show verbose")
    parser.add_argument('-l', '--log_conf', help='log config file',
        default=default_log_conf)
    parser.add_argument('-c', '--configfile', help='admin config file',
        default=default_admin_conf)
    parser.add_argument('-a', '--cephaddr', help='ceph mon addr')
    parser.add_argument('-n', '--name', dest='admin_user', default='admin',
        help='admin name for authentication')
    parser.add_argument('-k', '--keyfile', type=argparse.FileType('r'),
        help='admin keyfile for authentication')
    parser.add_argument('-x', '--prefix', help='path prefix', default='/')
    sub = parser.add_subparsers(title='support subcommands')
    sub.required = False

    listuser = sub.add_parser('ls', help='list user')
    listuser.set_defaults(func=lsuser_handler)

    getuser = sub.add_parser('get', help='get user')
    getuser.add_argument('user', help='user name')
    getuser.set_defaults(func=getuser_handler)

    adduser = sub.add_parser('add', help='add user to cephfs')
    adduser.add_argument('user', help='user name')
    adduser.add_argument('-p', '--paths', help='paths', nargs='+')
    adduser.add_argument('-g', '--groups', help='group names', nargs='+')
    adduser.add_argument('-q', '--quota', help='quota, max size',type=int)
    adduser.add_argument('-u', '--unit', help='quota, unit', default='g')
    adduser.add_argument('-o', '--outkey', help='output file of key',
        type=argparse.FileType('w'))
    adduser.set_defaults(func=adduser_handler)

    exportuser = sub.add_parser('export', help='export user info')
    exportuser.add_argument('user', help='user name')
    exportuser.add_argument('-r', '--root',
        help='root path')
    exportuser.add_argument('-i', '--rootindex', type=int, default=-1,
        help='index of root path, default -1')
    exportuser.add_argument('-o', '--infofile', type=argparse.FileType('w'),
        help='user info file')
    exportuser.set_defaults(func=exportuser_handler)

    updateuser = sub.add_parser('update', help='update user to cephfs, \
            support increment update')
    updateuser.add_argument('user', help='user name')
    updateuser.add_argument('-p', '--paths', help='paths', action='append')
    updateuser.add_argument('-p-', '--pathrm', help='paths to remove',
            action='append')
    updateuser.add_argument('-p+', '--pathadd', help='paths to add',
            action='append')
    updateuser.add_argument('-g', '--groups', help='group names', nargs='+')
    updateuser.add_argument('-q', '--quota', help='quota, max size',type=int)
    updateuser.add_argument('-u', '--unit', help='quota, unit', default='g')
    updateuser.set_defaults(func=updateuser_handler)

    deluser = sub.add_parser('del', help='del user')
    deluser.add_argument('user', help='user name')
    deluser.set_defaults(func=deluser_handler)

    show = sub.add_parser('show', help='show current admin info')
    show.set_defaults(func=show_handler)

    parsed_args = parser.parse_args(args)
    return parser, parsed_args

def main():
    if len(sys.argv)>1 and "-v" in sys.argv:
        print('cephadmin', adminI.version())
        return 0
    parser, parsed_args = parse_cmdargs()
    if parsed_args.log_conf:
        adminI.set_log_conf_file(parsed_args.log_conf)
    args = parsed_args.__dict__
    args.pop('log_conf')
    if parsed_args.keyfile:
        args['key'] = parsed_args.keyfile.read()
        parsed_args.keyfile.close()
    return parsed_args.func(**args)

if __name__ == "__main__":
    sys.exit(main())

