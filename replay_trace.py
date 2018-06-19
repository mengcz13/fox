import os
import sys
import subprocess
import argparse

BENCH = '/home/mengcz/fox/fox'
OUTPUTDIR = '/home/mengcz/fox/output'
INPUTDIR = '/home/mengcz/fox'
TRACES = ['macdrp', 'lammps_short']
ENGS = [5, 6, 7]

def main():
    if not os.path.exists(OUTPUTDIR):
        os.makedirs(OUTPUTDIR)

    parser = argparse.ArgumentParser()
    parser.add_argument('--default', action='store_true')
    parser.add_argument('--sbsize', action='store_true')
    parser.add_argument('--disksize', action='store_true')
    args = parser.parse_args()

    # default set
    if args.default:
        exprespath = os.path.join(OUTPUTDIR, 'default')
        if not os.path.exists(exprespath):
            os.makedirs(exprespath)
        for trace in TRACES:
            if trace == 'macdrp':
                nb, np = 62, 8 # for small IOs
            elif trace == 'lammps_short':
                nb, np = 62, 8 # for large IOs, cut
            for eng in ENGS:
                wd = os.path.join(exprespath, trace, str(eng))
                if not os.path.exists(wd):
                    os.makedirs(wd)
                inputtrace = os.path.join(INPUTDIR, 'input_%s.csv' % (trace))
                subp = subprocess.Popen("sudo %s run -d /dev/nvme0n1 -j 1 -c 8 -l 4 -b %d -p %d -r 0 -w 100 -v 8 -e %d -o -i %s" % (BENCH, nb, np, eng, inputtrace), shell=True, cwd=wd)
                subp.wait()

    # change sbsize
    if args.sbsize:
        exprespath = os.path.join(OUTPUTDIR, 'sbsize')
        if not os.path.exists(exprespath):
            os.makedirs(exprespath)
        for trace in TRACES:
            nb, np = 62, 8
            for npu, nblk in [(1, 1), (2, 1), (4, 1), (8, 1), (1, 2), (1, 4), (1, 8)]:
                eng = 7
                wd = os.path.join(exprespath, trace, str(eng), '%d_%d' % (npu, nblk))
                if not os.path.exists(wd):
                    os.makedirs(wd)
                inputtrace = os.path.join(INPUTDIR, 'input_%s.csv' % (trace))
                subp = subprocess.Popen("sudo %s run -d /dev/nvme0n1 -j 1 -c 8 -l 4 -b %d -p %d -r 0 -w 100 -v 8 -e %d -o --sb_pus %d --sb_blks %d -i %s" % (BENCH, nb, np, eng, npu, nblk, inputtrace), shell=True, cwd=wd)
                subp.wait()

    # change disk size
    if args.disksize:
        exprespath = os.path.join(OUTPUTDIR, 'disksize')
        if not os.path.exists(exprespath):
            os.makedirs(exprespath)
        for trace in TRACES:
            if trace == 'macdrp':
                np = 16
            elif trace == 'lammps_short':
                np = 32
            for nb in [16, 32, 62]:
                for eng in ENGS:
                    wd = os.path.join(exprespath, trace, str(eng), '%d_%d' % (nb, np))
                    if not os.path.exists(wd):
                        os.makedirs(wd)
                    inputtrace = os.path.join(INPUTDIR, 'input_%s.csv' % (trace))
                    subp = subprocess.Popen("sudo %s run -d /dev/nvme0n1 -j 1 -c 8 -l 4 -b %d -p %d -r 0 -w 100 -v 8 -e %d -o -i %s" % (BENCH, nb, np, eng, inputtrace), shell=True, cwd=wd)
                    subp.wait()


if __name__ == '__main__':
    main()
