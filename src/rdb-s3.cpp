extern "C" {
#include "rio.h"
#include "server.h"
}
#include <unistd.h>
#include <sys/wait.h>

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
extern "C" int rdbSaveS3(char *s3bucket, rdbSaveInfo *rsi)
{
    int status = EXIT_FAILURE;
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
        dup2(fd[0], STDIN_FILENO);
        close(fd[1]);
        close(fd[0]);
        execlp("aws", "aws", "s3", "cp", "-", s3bucket, nullptr);
        exit(EXIT_FAILURE);
    }
    else
    {
        close(fd[0]);
        if (rdbSaveFd(fd[1], rsi) != C_OK)
        {
            close(fd[1]);
            return C_ERR;
        }
        close(fd[1]);
        waitpid(pid, &status, 0);
    }
    
    if (status != EXIT_SUCCESS)
        serverLog(LL_WARNING, "Failed to save DB to AWS S3");
    else
        serverLog(LL_NOTICE,"DB saved on AWS S3");
        
    return (status == EXIT_SUCCESS) ? C_OK : C_ERR;
}