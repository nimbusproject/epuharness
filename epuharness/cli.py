import gevent.monkey ; gevent.monkey.patch_all()
import os
import sys

from harness import EPUHarness

ERROR_RETURN = 1

def usage(stdout=False):
    """Print usage information. Should normally print to stderr, except when
    called by --help, in which case it should print to stdout for pagers.
    """

    cli_name = os.path.basename(sys.argv[0])
    usage = """%s

usage:
   %s --help
   %s start [scheme1.yml]
   %s stop [--force]
""" % (cli_name, cli_name, cli_name, cli_name)

    if stdout:
        print usage
    else:
        print >> sys.stderr, usage

def normalize_arg(arg):
    arg = arg.lstrip('-')
    arg = arg.lower()
    return arg

def main(argv=None):

    epuharness = EPUHarness()

    if not argv:
        argv = list(sys.argv)

    action = ""
    try:
        appname = argv.pop(0)
        action = argv.pop(0)
    except IndexError:
        usage()
        sys.exit(ERROR_RETURN)

    action = normalize_arg(action)

    if action == 'help':
        usage(stdout=True)
    elif action == 'start':
        try:
            deployment_file = argv.pop(0)
        except IndexError:
            deployment_file = None
        epuharness.start(deployment_file)
    elif action == 'stop':
        force = False
        try:
            stoparg = argv.pop(0)
            stoparg = normalize_arg(stoparg)
            if stoparg == 'force':
                force = True
        except IndexError:
            pass

        epuharness.stop(force=force)
    else:
        usage()
        sys.exit(ERROR_RETURN)

