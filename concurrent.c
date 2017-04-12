/*

MIT License

Copyright (c) 2017 Sean Pringle, sean.pringle@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <err.h>
#include <errno.h>
#include <signal.h>
#include <poll.h>
#include <sys/wait.h>
#include <sys/prctl.h>

#define ensure(x) for ( ; !(x) ; exit(EXIT_FAILURE) )

int ignore = 0;
int verbose = 0;
int limit = 2;
int batch = 1;
int retry = 0;

const char *command;
char * const *cargs;

typedef struct _buf_t {
  char *data;
  int len;
} buf_t;

typedef struct _job_t {
  pid_t pid;
  int in;
  int out;
  buf_t outbuf;
  int err;
  buf_t errbuf;
  int retry;
  char **lines;
  int count;
  struct _job_t *next;
} job_t;

job_t *jobs;
int jobs_active;
uint64_t job_count;

#define READ 0
#define WRITE 1

void
write_exactly (int fd, const void *buf, size_t count)
{
  size_t written = 0;

  while (written < count)
  {
    size_t rc = write(fd, buf + written, count - written);

    if (rc < 0 && errno == EINTR)
      continue;

    ensure(rc >= 0)
      warnx("write_exactly() %d", errno);

    written += rc;
  }
}

void
read_buffered (int fd, buf_t *buf)
{
  while (poll(&(struct pollfd){ .fd = fd, .events = POLLIN }, 1, 0) == 1)
  {
    buf->data = realloc(buf->data, buf->len + 1024);
    int rc = read(fd, buf->data + buf->len, 1023);

    if (rc == 0)
      break;

    if (rc < 0 && errno == EINTR)
      continue;

    ensure(rc > 0)
      warnx("read_buffered() %d", errno);

    buf->len += rc;
  }
}

void
read_through (int src, int dst)
{
  char buf[1024];
  while (src)
  {
    int rc = read(src, buf, 1023);

    if (rc == 0)
      break;

    if (rc < 0 && errno == EINTR)
      continue;

    ensure(rc > 0)
      warnx("read_through() %d", errno);

    write_exactly(dst, buf, rc);
  }
}

pid_t
exec_piped (int *in, int *out, int *err)
{
  int p_stdin[2], p_stdout[2], p_stderr[2];

  ensure(
    pipe(p_stdin)  == 0 &&
    pipe(p_stdout) == 0 &&
    pipe(p_stderr) == 0
  );

  pid_t pid = fork();

  if (pid < 0)
    return pid;

  if (pid == 0)
  {
    ensure(
      close(p_stdin[WRITE]) == 0 && dup2(p_stdin[READ],   STDIN_FILENO ) == STDIN_FILENO  &&
      close(p_stdout[READ]) == 0 && dup2(p_stdout[WRITE], STDOUT_FILENO) == STDOUT_FILENO &&
      close(p_stderr[READ]) == 0 && dup2(p_stderr[WRITE], STDERR_FILENO) == STDERR_FILENO &&
      prctl(PR_SET_PDEATHSIG, SIGHUP) == 0
    );

    execvp(command, cargs);
    exit(EXIT_FAILURE);
  }

  *in  = p_stdin[WRITE];
  *out = p_stdout[READ];
  *err = p_stderr[READ];

  ensure(
    close(p_stdin[READ])   == 0 &&
    close(p_stdout[WRITE]) == 0 &&
    close(p_stderr[WRITE]) == 0
  );

  return pid;
}

pid_t
start_process (char **lines, int count, int *in, int *out, int *err)
{
  pid_t pid = exec_piped(in, out, err);

  for (int j = 0; j < count; j++)
  {
    int len = strlen(lines[j]);
    write_exactly(*in, lines[j], len);
  }

  ensure(close(*in) == 0);
  return pid;
}

void
clean_up (int sig)
{
  while (0 < waitpid(-1, NULL, WNOHANG));
}

pid_t
job_finish ()
{
  int status;
  pid_t pid = waitpid(-1, &status, WNOHANG);

  // Using wait() is insufficient as it's possible for all jobs to be
  // blocked writing to stdout/stderr. This semi-busy loop flip-flops
  // between buffering job output and waiting for the first exit.
  while (pid == 0)
  {
    for (job_t *j = jobs; j; j = j->next)
    {
      read_buffered(j->out, &j->outbuf);
      read_buffered(j->err, &j->errbuf);
    }

    usleep(1000);

    pid = waitpid(-1, &status, WNOHANG);
  }

  if (pid <= 0)
    return pid;

  job_t **pjob = &jobs;

  while (*pjob && (*pjob)->pid != pid)
    pjob = &((*pjob)->next);

  ensure(*pjob)
  {
    signal(SIGCHLD, clean_up);
    errx(EXIT_FAILURE, "unexpected job %d", pid);
  }

  jobs_active--;
  job_t *job = *pjob;

  if (job->errbuf.len > 0 && verbose)
    warnx("%d errbuf %d", pid, job->errbuf.len);

  // always relay errors
  if (job->errbuf.len > 0)
    write_exactly(STDERR_FILENO, job->errbuf.data, job->errbuf.len);

  // failure, but can retry
  if (status != 0 && job->retry > 0)
  {
    read_through(job->err, STDERR_FILENO);

    ensure(close(job->out) == 0);
    ensure(close(job->err) == 0);

    job->outbuf.data[0] = 0;
    job->outbuf.len = 0;

    job->errbuf.data[0] = 0;
    job->errbuf.len = 0;

    job->retry--;

    jobs_active++;
    job_count++;

    job->pid = start_process(job->lines, job->count, &(job->in), &(job->out), &(job->err));

    if (verbose)
      warnx("%d start (retry %d)", job->pid, pid);
  }
  else
  // success or ignorable failure
  if (status == 0 || (ignore && status != 0))
  {
    if (job->outbuf.len > 0 && verbose)
      warnx("%d outbuf %d", pid, job->outbuf.len);

    if (job->outbuf.len > 0)
      write_exactly(STDOUT_FILENO, job->outbuf.data, job->outbuf.len);

    read_through(job->err, STDERR_FILENO);
    read_through(job->out, STDOUT_FILENO);

    ensure(close(job->out) == 0);
    ensure(close(job->err) == 0);

    free(job->outbuf.data);
    free(job->errbuf.data);

    for (int i = 0; i < job->count; i++)
      free(job->lines[i]);

    free(job->lines);

    *pjob = job->next;
    free(job);

    if (verbose)
      warnx("%d %s", pid, status ? "fail (ignored)": "finish");
  }
  else
  // abort failure
  {
    read_through(job->err, STDERR_FILENO);

    signal(SIGCHLD, clean_up);
    errx(EXIT_FAILURE, "%d fail", pid);
  }

  return pid;
}

void
start_job (char **lines, int count)
{
  while (jobs_active >= limit)
    job_finish();

  job_t *job = calloc(1, sizeof(job_t));
  ensure(job) warnx("calloc fail %lu", sizeof(job_t));

  job->lines = lines;
  job->count = count;
  job->retry = retry;

  ensure((job->outbuf.data = calloc(1, 1024)) && (job->errbuf.data = calloc(1, 1024)))
    warnx("calloc fail 1024");

  jobs_active++;
  job_count++;

  job->pid = start_process(job->lines, job->count, &(job->in), &(job->out), &(job->err));

  if (verbose)
    warnx("%d start", job->pid);

  job->next = jobs;
  jobs = job;
}

char*
read_line (FILE *file)
{
  size_t chunk = 8192;
  size_t bytes = chunk;
  char *line = malloc(bytes+1);

  ensure(line)
    warnx("malloc fail %lu", bytes+1);

  line[0] = 0;

  while (!feof(file) && !ferror(file) && fgets(line + bytes - chunk, chunk+1, file) && !strchr(line + bytes - chunk, '\n'))
  {
    bytes += chunk;
    line = realloc(line, bytes+1);

    ensure(line)
      warnx("realloc fail %lu", bytes+1);
  }
  if (ferror(file) || (!line[0] && feof(file)))
  {
    free(line);
    line = NULL;
  }
  return line;
}

void
help()
{
  const char *text =
    "concurrent [-l N] [-b N] [-r N] [-i] COMMAND\n"
    "-l N : Limit concurrency to N jobs (default: #cores)\n"
    "-b N : Batch size of N lines (default: 1)\n"
    "-r N : Retry failed jobs N times (default: 0)\n"
    "-i   : Ignore job failures (default: abort)\n"
    "-v   : Verbose logging (default: off)\n";
  write_exactly(STDOUT_FILENO, text, strlen(text));
}

int
main (int argc, char const *argv[])
{
  limit = sysconf(_SC_NPROCESSORS_ONLN);

  for (int i = 1; i < argc; i++)
  {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0)
    {
      help();
      return EXIT_SUCCESS;
    }
    if ((strcmp(argv[i], "-l") == 0 || strcmp(argv[i], "--limit") == 0) && i < argc-1)
    {
      limit = atoi(argv[++i]);
      continue;
    }
    if ((strcmp(argv[i], "-b") == 0 || strcmp(argv[i], "--batch") == 0) && i < argc-1)
    {
      batch = atoi(argv[++i]);
      continue;
    }
    if ((strcmp(argv[i], "-r") == 0 || strcmp(argv[i], "--retry") == 0) && i < argc-1)
    {
      retry = atoi(argv[++i]);
      continue;
    }
    if ((strcmp(argv[i], "-i") == 0 || strcmp(argv[i], "--ignore") == 0))
    {
      ignore = 1;
      continue;
    }
    if ((strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0))
    {
      verbose = 1;
      continue;
    }
    if (argv[i][0] == '-')
    {
      errx(EXIT_FAILURE, "unexpected argument: %s", argv[i]);
    }

    command = (char*)argv[i];
    cargs = (char* const*) &argv[i];
    break;
  }

  ensure(limit > 0)
    errx(EXIT_FAILURE, "invalid limit %d", limit);

  ensure(batch > 0)
    errx(EXIT_FAILURE, "invalid batch %d", batch);

  ensure(retry >= 0)
    errx(EXIT_FAILURE, "invalid retry %d", retry);

  ensure(command)
    errx(EXIT_FAILURE, "missing command");

  if (verbose)
    warnx("limit %d batch %d retry %d ignore %d command %s", limit, batch, retry, ignore, command);

  char *line;
  char **lines;
  int count = 0;

  while ((line = read_line(stdin)))
  {
    if (!lines)
      lines = calloc(batch, sizeof(char*));

    lines[count++] = line;

    if (count == batch)
    {
      start_job(lines, count);
      count = 0;
      lines = NULL;
    }
  }

  if (count > 0)
  {
    start_job(lines, count);
    count = 0;
    lines = NULL;
  }

  while (jobs_active > 0)
    job_finish();

  if (verbose)
    warnx("jobs %lu", job_count);

  return EXIT_SUCCESS;
}
