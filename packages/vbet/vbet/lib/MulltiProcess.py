# SuperFastPython.com
# execute tasks in parallel in a for loop
from time import sleep
from random import random
import os
import tempfile
import traceback
from typing import List, Dict, NamedTuple
from collections.abc import Callable
import multiprocessing
from rscommons import Logger, TimerWaypoints, Timer
from rscommons.util import safe_makedirs, safe_remove_dir

# Typing hint for a type that is an array containing a tuple that has two elements of args and kwargs
MULTI_QUEUE = None


class MultiArgs(NamedTuple):
    args: List = []
    kwargs: Dict = {}


class MultiReturn(NamedTuple):
    task_id: str = 'TASK'
    process_index: int = 0
    error: Exception = None
    stacktrace: str = None
    logfile: str = None
    timer: int = 0


class MultiProcess:
    """
    More about this here: https://superfastpython.com/multiprocessing-for-loop/
    """

    def __init__(self,
                 task_id: str,
                 task: Callable,
                 multi_args: List[MultiArgs],
                 batch_size: int = 5,
                 debug=False,
                 no_multi=False,
                 temp_dir=None,
                 logger: Logger = None):

        self.task_id = task_id
        self.task = task
        self.multi_args = multi_args
        self.no_multi = no_multi
        self.debug = debug
        self.batch_size = batch_size
        self.temp_dir = temp_dir
        self.log = logger if logger else Logger(task_id)

    @staticmethod
    def _inner_func(task: Callable, proc_id: int, multi_args: MultiArgs, debug: bool, task_id: str, temp_dir: str):
        _tmr = Timer()
        # set up a temporary folder the inner function can use safely
        proc_temp_dir = tempfile.mkdtemp(prefix=f'{task_id}_', dir=temp_dir, )
        safe_makedirs(proc_temp_dir)

        # We need a separate log file for each process
        inner_log = Logger(task_id, multiproc_id=f'{task_id}{proc_id}')
        inner_log.setup(log_path=os.path.join(proc_temp_dir, f'{task_id}.log'))
        inner_log.title(f'Running {task_id} task in process {proc_id} (PID: {os.getpid()})')

        err = None
        stacktrace = None
        try:
            task(*multi_args.args, **multi_args.kwargs, workdir=proc_temp_dir, proc_log=inner_log)
        # pylint: disable=broad-except
        except Exception as excp:
            inner_log.error(f'Error in {task_id} task', excp)
            err = excp
            stacktrace = traceback.format_exc()
        finally:
            if not debug:
                safe_remove_dir(proc_temp_dir)

        # We have to set the return values explicitly
        MULTI_QUEUE.put(MultiReturn(
            task_id=task_id,
            process_index=proc_id,
            error=err,
            stacktrace=stacktrace,
            logfile=inner_log.instance.logpath,
            timer=_tmr.ellapsed()
        ))

    @ staticmethod
    def _init_worker(queue: multiprocessing.Queue):
        global MULTI_QUEUE
        MULTI_QUEUE = queue

    @ staticmethod
    def _watcher(proc_queue: multiprocessing.Queue, outer_log: Logger):
        # Now we need to assemble the logs again
        # first flush the logs so that anything in the buffer is written out
        finish_index = 0
        while True:
            ret_val: MultiReturn = proc_queue.get(True)
            # This is our signal that the watched should die
            # Put in a log line to summarize how this process did:
            if ret_val is None:
                break

            # Give us a nice title to know we started something
            with open(outer_log.instance.logpath, 'a', encoding='utf8') as log_f:
                if os.path.isfile(ret_val.logfile):
                    outer_log.flush()
                    with open(ret_val.logfile, 'r', encoding='utf8') as log_proc_f:
                        inner_text = log_proc_f.read()
                        log_f.write(inner_text)

                status = 'FAILED' if ret_val.error else 'succeeded'
                msg = f'---------- Process {ret_val.process_index} {status} in {finish_index} place in {ret_val.timer:.2f} seconds\n'
                log_f.write(msg)
                if ret_val.error is not None:
                    log_f.write(f'Error for process {ret_val.process_index} failed with error \n{ret_val.error}\n{ret_val.stacktrace}\n\n')
            finish_index += 1

    def run(self):
        self.log.info('Starting MultiProcess')
        multi_manager = multiprocessing.Manager()
        proc_queue = multi_manager.Queue()
        # Do the processing but first shut down the logfile writing so we don't clash
        # self.log.stop_filewrite()
        if self.no_multi:
            # We can turn off multiprocessing for debugging
            # loop over the multi_args and create a process for each batch
            for idx, (args, kwargs) in enumerate(self.multi_args):
                if self.no_multi:
                    self._inner_func(self.task, idx, self.task_id, self.temp_dir, self.debug, *args, **kwargs)
        else:
            with multiprocessing.Pool(self.batch_size, initializer=self._init_worker, initargs=(proc_queue,)) as pool:
                # Shut down the logging handler (for now)
                self.log.instance.logger.handlers.clear()
                # Create a watcher process to monitor the queue
                watcher_process = multiprocessing.Process(target=self._watcher, args=(proc_queue, self.log))
                watcher_process.start()

                # create a mapped process with indeces
                # _inner_func(task: Callable, proc_id: int, multi_args: MultiArgs, debug: bool, task_id: str, temp_dir: str):
                result = pool.starmap_async(
                    self._inner_func,
                    [(self.task, idx, args, self.debug, self.task_id, self.temp_dir) for idx, args in enumerate(self.multi_args)],
                )
                result.wait()
                if not result.successful():
                    self.log.error('MultiProcess failed')
                    raise result.get()
                pool.close()
                pool.join()
                proc_queue.put(None)
                # This is our signal that the watcher will not get any more
                # Now wait for the pool to wind down along with the watcher
                watcher_process.join()
                # Start the log handler back up
                self.log.setup(log_path=self.log.instance.logpath, verbose=self.log.instance.verbose, mode='a')

        # Now we need to boot up file writing again in the logs
        # self.log.start_filewrite()
        self.log.info('MultiProcess complete')


def taskfunc(taskname: str, random_fail: bool = False, workdir=str, proc_log=Logger):
    """This is just a dummy function to simulate some work

    Args:
        arg (_type_): _description_
        workdir (_type_, optional): _description_. Defaults to str.
        proc_log (_type_, optional): _description_. Defaults to Logger.
    """
    proc_log.info(f'[taskfunc]: Starting task with arg {taskname}')
    # generate a random value between 0 and 5
    value = random() * 2
    fail = random() > 0.5
    # block for a fraction of a second
    sleep(value)
    if fail and random_fail:
        proc_log.error(f'Random failure for {taskname}')
        raise ValueError(f'Random failure for {taskname}')
    with open(os.path.join(workdir, f'{taskname}.txt'), 'w', encoding='utf8') as f_stub:
        f_stub.write(f'{taskname} {value}')

    # report a message
    proc_log.info(f'[taskfunc]: Done with task {taskname} in {value:.2f} seconds')


# protect the entry point
if __name__ == '__main__':
    outer_workdir = os.path.join(os.environ.get('DATA_ROOT', None), 'MultiProcessingExperiment')
    safe_remove_dir(outer_workdir)
    safe_makedirs(outer_workdir)

    log = Logger('test')
    log.setup(log_path=os.path.join(outer_workdir, 'outer_log.log'), verbose=True)
    log.info(f'Temp dir: {outer_workdir}')

    _tmr_wpts = TimerWaypoints()
    work_items = [f'WorkItem__{idx}' for idx in range(20)]

    # Ok, first let's do it the old way
    log.title('Old Way')
    old_way_dir = os.path.join(outer_workdir, 'old_way')
    safe_makedirs(old_way_dir)

    # This is how we used to do things
    # ------------------------------------------------------------------------------
    for i in work_items:
        taskfunc(i, workdir=old_way_dir, proc_log=log)
    # ------------------------------------------------------------------------------
    _tmr_wpts.timer_break('old way')

    # Now here's the new hotness
    log.info('\n\n\n\n\n')
    log.title('New Way')
    new_way_dir = os.path.join(outer_workdir, 'new_way')
    safe_makedirs(new_way_dir)
    # ------------------------------------------------------------------------------
    processor = MultiProcess('test',
                             taskfunc,
                             [MultiArgs(args=[i]) for i in work_items],
                             batch_size=5,
                             debug=True,
                             temp_dir=new_way_dir,
                             logger=log
                             )
    # ------------------------------------------------------------------------------
    processor.run()
    _tmr_wpts.timer_break('new way batch size 5')

    # try it again with batch size of 1
    log.info('\n\n\n\n\n')
    log.title('New Way but with a thread pool of 1')
    new_way_dir1 = os.path.join(outer_workdir, 'new_way_1')
    safe_makedirs(new_way_dir1)
    processor = MultiProcess('test',
                             taskfunc,
                             [MultiArgs(args=[i]) for i in work_items],
                             batch_size=1,
                             temp_dir=new_way_dir1,
                             debug=True)
    processor.run()
    _tmr_wpts.timer_break('new way batch size 1')

    # try it again with batch size of 20
    log.info('\n\n\n\n\n')
    log.title('New Way but with a thread pool of 20')
    new_way_dir20 = os.path.join(outer_workdir, 'new_way_20')
    safe_makedirs(new_way_dir20)
    processor = MultiProcess('test',
                             taskfunc,
                             [MultiArgs(args=[i]) for i in work_items],
                             batch_size=20,
                             temp_dir=new_way_dir20,
                             debug=True)
    processor.run()
    _tmr_wpts.timer_break('new way batch size 20')

    # try it again with failures
    log.info('\n\n\n\n\n')
    log.title('New Way but with failures')
    processor = MultiProcess('test',
                             taskfunc,
                             [MultiArgs(args=[i], kwargs={"random_fail": True}) for i in work_items],
                             batch_size=5,
                             debug=True)
    processor.run()
    _tmr_wpts.timer_break('new way with failures')

    # Now print out a summary
    log.info(f'\n\n\n{_tmr_wpts.toString()}')
