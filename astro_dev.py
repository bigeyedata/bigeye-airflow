import fileinput
import logging as logging
import os
import shutil
import sys
import typer

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


def get_compatability_path(version: str) -> str:
    return f'airflow{version}'


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
def restart(airflow_compatibility_version: str = typer.Option(
        '1'
        , "--airflow_compatibility_version"
        , "-v"
        , help="Airflow Compatibility Version.")):
    stop(airflow_compatibility_version)
    start(airflow_compatibility_version)


@app.command()
def start(airflow_compatibility_version: str = typer.Option(
        '1'
        , "--airflow_compatibility_version"
        , "-v"
        , help="Airflow Compatibility Version.")):

    log.info('Copying solution')
    copy_and_overwrite(
        f'{get_compatability_path(airflow_compatibility_version)}/bigeye_airflow/',
        f'{get_compatability_path(airflow_compatibility_version)}/astro/plugins/bigeye_airflow')

    log.info('Starting astro...')
    start_cmd = 'astro dev start'
    run_subproc(start_cmd, f'./{get_compatability_path(airflow_compatibility_version)}/astro')


@app.command()
def stop(airflow_compatibility_version: str = typer.Option(
        '1'
        , "--airflow_compatibility_version"
        , "-v"
        , help="Airflow Compatibility Version.")):
    log.info('Stopping astro...')
    cmd = 'astro dev stop'
    run_subproc(cmd, f'./{get_compatability_path(airflow_compatibility_version)}/astro')


if __name__ == '__main__':
    app()
