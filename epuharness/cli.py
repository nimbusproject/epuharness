import gevent.monkey ; gevent.monkey.patch_all()
import os
import sys
import logging
try:
    import argparse
except ImportError:
    #TODO add argparse to setup.py for pre 2.7
    print "Couldn't import argparse. Use Python 2.7"


from harness import EPUHarness
from exceptions import HarnessException

log = logging.getLogger(__name__)

ERROR_RETURN = 1

def main(argv=None):


    if not argv:
        argv = list(sys.argv)
    command = argv.pop(0)

    parser = argparse.ArgumentParser("Start EPU Services Locally")
    parser.add_argument('-f', '--force', action='store_true')
    parser.add_argument('-x', '--exchange', metavar='EXCHANGE_NAME',
            default=None)
    parser.add_argument('action', metavar='ACTION', help='start or stop')
    parser.add_argument('extras', help='deployment config file for start, or services to stop',
            default=[], nargs='*')
    args = parser.parse_args(argv)

    epuharness = EPUHarness(exchange=args.exchange)

    action = args.action.lower()
    if action == 'start':
        configs = args.extras
        if len(configs) > 0:
            config = configs[0]
            if config.endswith('.yml') or config.endswith('.json'):
                deployment_file = config
            else:
                print >>sys.stderr, "Your configuration file isn't recognized"
                sys.exit(ERROR_RETURN)
        else:
            deployment_file = None

        try:
            epuharness.start(deployment_file)
        except HarnessException, e:
            log.error("Problem starting services: %s" % e.message)
            sys.exit(ERROR_RETURN)
    elif action == 'stop':
        services = getattr(args, 'extras')
        force = args.force
        try:
            epuharness.stop(force=force, services=services)
        except HarnessException, e:
            log.error("Problem stopping services: %s" % e.message)
            sys.exit(ERROR_RETURN)
    elif action == 'status':
        try:
            epuharness.status()
        except HarnessException, e:
            log.error("Problem getting status: %s" % e.message)
            sys.exit(ERROR_RETURN)
    else:
        usage()
        sys.exit(ERROR_RETURN)

