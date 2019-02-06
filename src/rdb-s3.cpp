extern "C" {
#include "rio.h"
#include "server.h"
}
#include <unistd.h>
#include <sys/wait.h>

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
extern "C" int rdbSaveS3(char *s3bucket, rdbSaveInfo *rsi)
{
    int fd[2];
    if (pipe(fd) != 0)
        return C_ERR;

    pid_t pid = fork();
    if (pid < 0)
    {
        close(fd[0]);
        close(fd[1]);
        return C_ERR;
    }

    if (pid == 0)
    {
        // child process
        dup2(fd[1], STDIN_FILENO);
        execlp("aws", "s3", "cp", "-", s3bucket, nullptr);
        exit(EXIT_FAILURE);
    }
    else
    {
        close(fd[1]);
        rdbSaveFd(fd[0], rsi);
        int status;
        waitpid(pid, &status, 0);
    }
    
    close(fd[0]);
    
    
    // NOP
    return C_ERR;
}