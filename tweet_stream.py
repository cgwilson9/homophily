#!/usr/bin/env python3
"""Collect tweets from Twitter streaming API via tweepy"""

import argparse
import datetime
import gzip
import sys

from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener


class CustomListener(StreamListener):
  def __init__(self, write=print):
    super(CustomListener, self).__init__()
    self.write = write

  def on_data(self, raw_data):
    self.write(raw_data)

  def on_error(self, status_code):
    eprint(status_code)


def eprint(*args, **kwargs):
  """Print to stderr"""
  print(*args, file=sys.stderr, **kwargs)


if __name__ == "__main__":
  # Set up the argument parser
  parser = argparse.ArgumentParser(
      description="Fetch data with Twitter Streaming API")
  parser.add_argument("--keyfile",
                      help="file with user credentials",
                      required=True)
  parser.add_argument("--gzip",
                      metavar="OUTPUT_FILE",
                      help="file to write compressed results to")
  parser.add_argument("--filter", metavar="W", nargs="*",
                      help="space-separated list of words;"
                      "tweets matching any word in the list are returned")
  flags = parser.parse_args()

  # Read twitter app credentials and set up authentication
  creds = {}
  for line in open(flags.keyfile, "r"):
    row = line.strip()
    if row:
      key, value = row.split()
      creds[key] = value

  auth = OAuthHandler(creds["api_key"], creds["api_secret"])
  auth.set_access_token(creds["token"], creds["token_secret"])

  # Write tweets to stdout or a gzipped file, as requested
  if flags.gzip:
    # Write to gzipped file
    f = gzip.open(flags.gzip, "wt")
    eprint("Writing gzipped output to %s" % flags.gzip)
    output = f.write

  else:
    # write to stdout
    output = print

  # Track time and start streaming
  starttime = datetime.datetime.now()
  twitterstream = Stream(auth, CustomListener(write=output))

  eprint("Started running at", starttime)

  while True:
    try:
      if flags.filter:
        # Track specific tweets
        twitterstream.filter(track=flags.filter, languages=["en"])
      else:
        # Get random sample of tweets
        twitterstream.sample()
    except KeyboardInterrupt:
      eprint()
      break
    except AttributeError:
      # Catch rare occasion when Streaming API returns None
      pass

  if flags.gzip:
    eprint("Closing %s" % flags.gzip)
    f.close()

  eprint("Total run time", datetime.datetime.now() - starttime)
