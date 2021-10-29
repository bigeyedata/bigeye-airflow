import fileinput
import os
import shutil
import sys

import typer

import logging as logging

import version

app = typer.Typer()

USER = os.getlogin()
WK_DIR = os.getcwd()

# create logger
log = logging.getLogger(__file__)
log.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
log.addHandler(ch)


def delete(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)


def copy_and_overwrite(from_path: str, to_path: str):
    delete(to_path)
    shutil.copytree(from_path, to_path)


def run_subproc(cmd: str, run_path: str = None):
    import subprocess

    if run_path:
        os.chdir(run_path)
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if run_path:
        os.chdir(WK_DIR)
    if output:
        log.info(output)
    if error:
        log.error(error)


def file_replace_all_lines_contains(file: str, search_exp: str, replace_exp: str):
    for line in fileinput.input(file, inplace=1):
        if search_exp in line:
            line = line.replace(search_exp, replace_exp)
        sys.stdout.write(line)


@app.command()
def restart():
    stop()
    start()


@app.command()
def start():
    # log.info('Building wheel...')
    # build_wheel_cmd = 'python3 setup.py bdist_wheel'
    # run_subproc(build_wheel_cmd)
    #
    # log.info('Copying wheel to astro/include...')
    # delete('astro/include/bigeye-airflow-*.whl')
    # copy_and_overwrite(f'dist/bigeye_airflow-{version.__version__}-py3-none-any.whl', 'astro/include/')

    # log.info('Updating version in astor/requirements.txt')
    # file_replace_all_lines_contains('astro/requirements.txt'
    #                                 , 'include/bigeye_airflow-'
    #                                 , 'include/bigeye_airflow-{version.__version__}-py3-none-any.whl')

    log.info('Copying solution')
    copy_and_overwrite(f'bigeye_airflow/', 'astro/plugins/bigeye_airflow')

    log.info('Starting astro...')
    start_cmd = 'astro dev start'
    run_subproc(start_cmd, './astro')


@app.command()
def stop():
    log.info('Stopping astro...')
    cmd = 'astro dev stop'
    run_subproc(cmd, './astro')


if __name__ == '__main__':
    app()
