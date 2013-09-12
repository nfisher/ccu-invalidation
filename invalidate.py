#!/usr/bin/env python

"""
Utility script to invalidate Akamai ARL's.

Example:

  invalidate.py user password http://client.dowload.akamai.com/invalidateme/

"""

import httplib
import base64
import json
import socket
import sys
import time

AKAMAI_HOST='api.ccu.akamai.com'
AKAMAI_PATH='/ccu/v2/queues/default'

QERR_RC=1       # Queue error exit code.
SOCKERR_RC=2    # Socket connection exit code.
ARGS_RC=3       # Incorrect number of arguments exit code.
PURGE_RC=4      # Purge error exit code.
STATUS_RC=5     # Purge status error exit code.
UNKNOWN_RC=6    # Unknown status error exit code.
EXHAUST_RC=7    # Unknown retries exhuasted exit code.
PEBKAC_RC=8     # User cancelled execution CTRL-C.


def get(conn, path, auth):
    """ Returns GET response.

    Make a GET request to the URL specified by path.

    conn - HTTPS connection to Akamai Web Services.
    path - URL path to get.
    auth - base64 encoded authentication string.

    """
    conn.putrequest("GET", path)
    conn.putheader("Authorization", "Basic {0}".format(auth))
    conn.endheaders()
    conn.send('')
    return conn.getresponse()


def post(conn, path, auth, post_body):
    """ Return POST response.

    Make a POST request to the URL specified by path with the post body.

    conn - HTTPS connection to Akamai Web Services.
    path - URL path to get.
    auth - base64 encoded authentication string.
    post_body - the post body.

    """
    conn.putrequest("POST", path)
    conn.putheader("Authorization", "Basic {0}".format(auth))
    conn.putheader("Content-Type",    "application/json")
    conn.putheader("Content-Length", len(post_body))
    conn.endheaders()
    conn.send(post_body)
    return conn.getresponse()


def get_queue_size(conn, auth):
    """ Returns nothing.

    Queries for the current queue size.

    conn - HTTPS connection to Akamai Web Services.
    path - URL path to get.

    """
    queue_resp = get(conn, AKAMAI_PATH, auth)
    if (queue_resp.status != 200):
        sys.stderr.write("Error retrieving queue size HTTP response is {0}\n".format(queue_resp.status))
        exit(QERR_RC)

    queue_json = json.loads(queue_resp.read())
    print "Current queue length {0}.".format(queue_json["queueLength"])


def invalidate_arl(conn, auth, arls):
    """ Returns the results of the request as a dictionary.

    Invalidate ARL's using Akamai Web Services REST API.

    conn - HTTPS connection to Akamai Web Services.
    auth - base64 encoded authentication string.
    arls - list of arls to invalidate.

    """
    # Prepare the invalidate URL request.
    payload_dict = dict()
    payload_dict['action'] = 'invalidate'
    payload_dict['objects'] = arls
    payload = json.dumps(payload_dict)
    purge_resp = post(conn, AKAMAI_PATH, auth, payload)

    if (purge_resp.status != 201):
        sys.stderr.write("Error purging {0}\n".format(purge_resp.status))
        print purge_resp.read()
        exit(PURGE_RC)

    purge_json = json.loads(purge_resp.read())
    sleep_time = purge_json.get('pingAfter', 420)
    conn.close() # close the connection to prevent an invalid sleep due to the sleep below.

    print "Invalidate submitted, will check purge status in {0}s.".format(sleep_time)
    time.sleep(sleep_time)

    return purge_json


def check_response(conn, auth, progress_path):
    """ Returns nothing.

    Poll the specified progress_path for the status of the submitted job.

    conn - HTTPS connection to Akamai Web Services.
    auth - base64 encoded authentication string.
    progress_path - the progress URI path.

    """
    unknown_retries = 0
    # Loop with a sleep until the purge request is marked complete.
    while (True):
        conn.connect()
        check_resp = get(conn, progress_path, auth)

        # if HTTP response code isn't 200 somethings wrong, exit.
        if (check_resp.status != 200):
            sys.stderr.write("Error retrieving queue status HTTP response is {0}\n".format(check_resp.status))
            exit(STATUS_RC)

        check_json = json.loads(check_resp.read())
        conn.close() # close the connection to prevent an invalid sleep due to the sleep below.

        if (check_json.get('purgeStatus', '') == 'Done'):
            print "Jobs done, going home!"
            break

        elif (check_json.get('purgeStatus', '') == 'Unknown'):
            sys.stderr.write("Job is unknown, will sleep 1m and try again ({0} of 3).".format(unknown_retries))
            time.sleep(60)
            unknown_retries += 1

            if (unknown_retries >= 3):
                sys.stderr.write("Exhausted retries for unknown job, exiting.")
                exit(EXHAUST_RC)

        elif (check_json.get('purgeStatus', '') == 'In-Progress'):
            # TODO: (NF 2013-09-12) probably want to limit the number of times to requery.
            sleep_time = check_json.get('pingAfter', 420)
            print "Sleeping, will check purge status in {0}s.".format(sleep_time)
            time.sleep(sleep_time)

        else:
            sys.stderr.write("Job is unknown, not sure how to respond: {0}\n".format(check_json))
            exit(UNKNOWN_RC)

def main():
    # Check number of arguments, print out usage and exit if not at least 4.
    if (len(sys.argv) < 4):
        print """
Usage: invalidate.py USER PASSWORD ARL

USER     - AWS username.
PASSWORD - AWS password.
ARL      - Akamai ARL to invalidate.

Example:

  invalidate.py user password http://client.dowload.akamai.com/invalidateme/
        """
        exit(ARGS_RC)

    try:
        # encode username and password from the command line arguments.
        user=sys.argv[1]
        password=sys.argv[2]
        auth=base64.standard_b64encode("{0}:{1}".format(user, password))
        arls = sys.argv[3:]

        conn = httplib.HTTPSConnection(AKAMAI_HOST)

        get_queue_size(conn, auth)
        purge_json = invalidate_arl(conn, auth, arls)
        check_response(conn, auth, purge_json['progressUri'])
        conn.close()

    except socket.gaierror:
        print "Socket error occurred, unable to connect."
        exit(SOCKERR_RC)

    except socket.error:
        print "Socket error occurred, unable to connect."
        exit(SOCKERR_RC)
    
    except KeyboardInterrupt:
        print "User cancelled, some people need some patience! ;)"
        exit(PEBKAC_RC)

if __name__ == "__main__":
    main()
