import gevent.monkey ; gevent.monkey.patch_all()
import os
import sys
try:
    import argparse
except ImportError:
    #TODO add argparse to setup.py for pre 2.7
    print "Couldn't import argparse. Use Python 2.7"


from harness import EPUHarness

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
    parser.add_argument('config.yml', help='deployment config file',
            default=[], nargs='?')
    args = parser.parse_args(argv)

    epuharness = EPUHarness(exchange=args.exchange)

    action = args.action.lower()
    if action == 'start':
        try:
            config = getattr(args, 'config.yml')
            if config.endswith('.yml') or config.endswith('.json'):
                deployment_file = config
            else:
                config = config[0]
                if config.endswith('.yml') or config.endswith('.json'):
                    deployment_file = config
                else:
                    print >>sys.stderr, "Your configuration file isn't recognized"
                    sys.exit(ERROR_RETURN)
        except AttributeError:
            deployment_file = None

        epuharness.start(deployment_file)
    elif action == 'stop':
        force = args.force
        epuharness.stop(force=force)
    else:
        usage()
        sys.exit(ERROR_RETURN)

