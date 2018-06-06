import os
import sys
import subprocess

BENCH = '/home/mengcz/fox/fox'
OUTPUTDIR = '/home/mengcz/fox/output'
INPUTDIR = '/home/mengcz/fox'
TRACES = ['macdrp', 'lammps']
ENGS = [5, 6, 7]

def main():
    if not os.path.exists(OUTPUTDIR):
        os.makedirs(OUTPUTDIR)

    # default set
    exprespath = os.path.join(OUTPUTDIR, 'default')
    if not os.path.exists(exprespath):
        os.makedirs(exprespath)
    for trace in TRACES:
        for eng in ENGS:
            wd = os.path.join(exprespath, trace, str(eng))
            if not os.path.exists(wd):
                os.makedirs(wd)
            inputtrace = os.path.join(INPUTDIR, 'input_%s.csv' % (trace))
            subp = subprocess.Popen("sudo %s run -d /dev/nvme0n1 -j 1 -c 8 -l 4 -b 32 -p 64 -r 0 -w 100 -v 8 -e %d -i %s" % (BENCH, eng, inputtrace), shell=True, cwd=wd)
            subp.wait()

if __name__ == '__main__':
    main()
