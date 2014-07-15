#!/usr/bin/env python

import os
import time
import tarfile
import heapq
import pickle
import argparse

class TarInfoCmpWrapper:
    def __init__(self, tarinfo):
        self._tarinfo = tarinfo
    def unwrap(self):
        return self._tarinfo
    def __cmp__(self, other):
        return cmp(other.unwrap().name, self.unwrap().name)

class TarballMembersHeap:
    def __init__(self, tarball_members):
        self._heap = [TarInfoCmpWrapper(m) for m in tarball_members]
        heapq.heapify(self._heap)
        self._popped = None
        self.pop()

    def top(self):
        return self._popped

    def pop(self):
        res = self._popped
        self._popped = heapq.heappop(self._heap).unwrap() if self._heap else None
        return res

def tarball_diff(tarball_members, new_tarball_members):
    members = TarballMembersHeap(tarball_members)
    new_members = TarballMembersHeap(new_tarball_members)
    while True:
        if not members.top():
            while new_members.top():
                yield (None, new_members.pop())
            break
        if not new_members.top():
            while members.top():
                yield (members.pop(), None)
            break
        cmpres = cmp(members.top().name, new_members.top().name)
        if 0 == cmpres:
            (m, new_m) = (members.pop(), new_members.pop())
            if m.tobuf() != new_m.tobuf():
                yield (m, new_m)
        elif cmpres > 0:
            yield (members.pop(), None)
        else:
            yield (None, new_members.pop())



def update_tarball(path, root, dbpath):
    tarball = tarfile.open(path)
    name = os.path.basename(path)
    dbfilename = get_dbfilename(dbpath, name)
    if os.path.isfile(dbfilename):
        print "updating {}...".format(name)
        update_members = []
        with open(dbfilename) as dbfile:
            old_members = pickle.load(dbfile)
        for (old_m, new_m) in tarball_diff(old_members, tarball.getmembers()):
            if new_m:
                new_fname = os.path.join(root, new_m.name)
                if not os.path.isdir(new_fname):
                    print "\tupdate {} {}".format(new_fname, time.ctime(new_m.mtime))
                    update_members.append(new_m)
            else:
                old_fname = os.path.join(root, old_m.name)
                if not os.path.isdir(old_fname):
                    print "\tremove {}".format(old_fname)
                    os.remove(old_fname)

            if update_members:
                tarball.extractall(path=root, members=update_members)
    else:
        print "installing {}...".format(name)
        tarball.extractall(path=root)
    print "OK"
    pickle.dump(tarball.getmembers(), open(dbfilename,"w"))

def action_update(args):
    dbpath = get_dbpath(args)
    if not os.path.isdir(dbpath):
        os.makedirs(dbpath)
    for path in args.tarball:
        update_tarball(path, root=args.root, dbpath=dbpath)

def delete_tarball(name, root, dbpath):
    db_fname = get_dbfilename(dbpath, name)
    with open(db_fname, "r") as f:
        members = pickle.load(f)
    for m in members:
        fname = os.path.join(root, m.name)
        if not os.path.isdir(fname):
            os.remove(fname)
    os.remove(db_fname)

def action_delete(args):
    for path in args.tarball:
        delete_tarball(os.path.basename(path), root=args.root, dbpath=get_dbpath(args))

def action_list(args):
    tarballs = [get_tarballname(f) for f in os.listdir(get_dbpath(args))]
    print "\n".join(sorted(tarballs))

def get_dbfilename(dbpath, tarball_name):
    return os.path.join(dbpath, tarball_name + ".info")

def get_tarballname(dbfilename):
    if not dbfilename.endswith(".info"):
        raise Exception("not a db file name: '{}'".format(dbfilename))
    return os.path.basename(dbfilename)[:-5]

def get_dbpath(args):
    return os.path.join(args.root, ".tarballpkgdb")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.getcwd(),
            help="root directory, it is current directory by default")
    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument("tarball", nargs="+")
    parser_update.set_defaults(func=action_update)

    parser_update = subparsers.add_parser('delete')
    parser_update.add_argument("tarball", nargs="+")
    parser_update.set_defaults(func=action_delete)

    parser_update = subparsers.add_parser('list')
    parser_update.set_defaults(func=action_list)
    args = parser.parse_args()
    args.func(args)

if __name__=="__main__":
    main()

