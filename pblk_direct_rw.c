#define _GNU_SOURCE
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>

static int BLKSIZE = 4096;

struct iorec {
    int offset;
    int size;
};

struct iorec iorecs[100000];

int load_iorecs(char* filename, struct iorec* iorecs) {
    FILE* pfile = fopen(filename, "r");
    if (pfile == NULL)
        return 1;
    int t_offset, t_size, t_recnum;
    char t_iotype;
    if (fscanf(pfile, "%d", &t_recnum) == EOF)
        return 1;
    int t_recnum_i;
    for (t_recnum_i = 0; t_recnum_i < t_recnum; t_recnum_i++) {
        fscanf(pfile, "%d,%d,%c", &t_offset, &t_size, &t_iotype);
        iorecs[t_recnum_i].offset = t_offset;
        iorecs[t_recnum_i].size = t_size;
        // printf("%d,%d\n", t_offset, t_size);
    }
    fclose(pfile);
    return t_recnum;
}

int main() {
    int recnum = load_iorecs("./input_lammps_short.csv", iorecs);
    int maxiosize = 0;
    int i;
    for (i = 0; i < recnum; i++) {
        if (iorecs[i].size > maxiosize)
            maxiosize = iorecs[i].size;
    }

    int fd;
    fd = open("/dev/mydevice", O_CREAT | O_TRUNC | O_DIRECT | O_RDWR, 0644);
    if (fd == -1)
        return -1;

    size_t aligned_maxsize = (maxiosize + BLKSIZE - 1) / BLKSIZE * BLKSIZE;
    char* buf = malloc(aligned_maxsize + BLKSIZE * 2);

    printf("maxiosize: %d\n", maxiosize);
    struct timeval tvalst, tvaled;
    gettimeofday(&tvalst, NULL);
    uint64_t totalbytes = 0;
    for (i = 0; i < recnum; i++) {
        size_t aligned_file_size = (iorecs[i].size + BLKSIZE - 1) / BLKSIZE * BLKSIZE;
        size_t aligned_offset = (iorecs[i].offset + BLKSIZE - 1) / BLKSIZE * BLKSIZE;
        char* tempbuf = buf;
        char* alignedbuf = (char*)(((uintptr_t)tempbuf + BLKSIZE - 1) / BLKSIZE * BLKSIZE);
        int rets = lseek(fd, aligned_offset, SEEK_SET);
        if (rets == -1) {
            printf("lseek error: %s\n", strerror(errno));
            close(fd);
            free(buf);
            return -1;
        }
        int ret = write(fd, alignedbuf, aligned_file_size);
        if (ret != aligned_file_size) {
            printf("aligned file size: %d vs %d\n", aligned_file_size, iorecs[i].size);
            printf("write error: %s\n", strerror(errno));
            close(fd);
            free(buf);
            return -2;
        }
        totalbytes += (uint64_t)aligned_file_size;
        if (i % 100 == 1 || i == recnum - 1) {
            gettimeofday(&tvaled, NULL);
            uint64_t exetime = ((uint64_t)(tvaled.tv_sec - tvalst.tv_sec) * 1000000L + tvaled.tv_usec) - tvalst.tv_usec;
            double spd = (double)totalbytes / (double)exetime * 1e6 / (1024 * 1024);
            printf("[%d/%d], %" PRId64 ", %" PRId64 ", %.3lf\n", i + 1, recnum, totalbytes, exetime, spd);
        }
    }

    close(fd);
    free(buf);
    return 0;
}
