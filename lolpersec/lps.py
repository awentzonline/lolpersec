from gevent.monkey import patch_all; patch_all()

import os
import time
from collections import Counter

import gevent
import gipc
import matplotlib
from gevent.queue import Queue
from gtwittools.gutils import (
    echo_queue, fanout, never_surrender, sampler,
    spawn_greenlets, spawn_processes
)
from gtwittools.tweetin import (
    extract_statuses, filter_twitter, get_twitter_api
)


matplotlib.use('Agg')

DEBUG = bool(os.environ.get('LPS_DEBUG', None))

# sampler process

TWEET_INTERVAL = int(os.environ.get('TWEET_INTERVAL', 0)) or 60.0 * 15
TWEET_SAMPLE_WINDOW = int(os.environ.get('TWEET_SAMPLE_WINDOW', 0)) or 5.0  # seconds

counter = 0
last_t = time.time()
top_tweet = None


def sample_counter():
    global counter, last_t
    
    while True:
        t = time.time()
        dt = t - last_t
        last_t = t
        count = counter
        counter = 0
        if not dt:
            dt = 1.0
        yield count / dt


def count_phrases(phrase_q, phrase):
    global counter
    for text in phrase_q:
        counter += text.count(phrase)


class TopTweet(object):
    def __init__(self):
        self.this_counter = Counter()
        self.last_counter = Counter()

    @property
    def top_tweet_url(self):
        counts = self.windowed_counts
        top = counts.most_common(1)
        if top:
            return top[0][0]  # strip off count
        return None

    @property
    def windowed_counts(self):
        counts = Counter(self.this_counter)
        counts.update(self.last_counter)
        return counts

    def update(self, url):
        self.this_counter[url] += 1

    def advance_window(self):
        self.last_counter = self.this_counter
        self.this_counter = Counter()


top_tweet = TopTweet()


def count_top_tweet(status_q):
    """Find the most replied to tweet."""
    global top_tweet

    for item in status_q:
        replied_id = item.get('in_reply_to_status_id')
        if not replied_id:
            continue
        user_name = item.get('in_reply_to_screen_name', 'foo')
        url = "https://twitter.com/{}/status/{}".format(
            user_name, replied_id
        )
        top_tweet.update(url)
        if DEBUG:
            print url


def aggregate_sampler_data(output_writer, buffer_q):
    for buffer_item in buffer_q:
        top_tweet_url= top_tweet.top_tweet_url
        top_tweet.advance_window()
        output_writer.put((buffer_item, top_tweet_url))


def sampler_process(output_writer, fn, phrase='lol'):
    sample_buffer_q = Queue()
    raw_status_q = Queue()
    status_q = Queue()
    phrase_status_q = Queue()
    top_tweet_status_q = Queue()
    twitter_api = get_twitter_api()
    confs = [
        [sampler, sample_buffer_q, fn, TWEET_SAMPLE_WINDOW, TWEET_INTERVAL],
        [aggregate_sampler_data, output_writer, sample_buffer_q],
        [filter_twitter, twitter_api, status_q, [phrase]],
        [fanout, status_q, [phrase_status_q, top_tweet_status_q]],
        [extract_statuses, phrase_status_q, raw_status_q],
        [count_phrases, raw_status_q, phrase],
        # top tweet
        [count_top_tweet, top_tweet_status_q]
    ]
    if not DEBUG:
        for conf in confs:
            conf[0] = never_surrender(conf[0])
    spawn_greenlets(confs)


# renderer process

def plotter(conf_q, rendered_q, output_dir='/tmp/'):
    import os
    import shutil
    import tempfile

    if not output_dir:
        output_dir = tempfile.mkdtemp()
    num_plots = 0
    for plot_conf in conf_q:
        caption = plot_conf.pop('caption')

        if not caption:
            caption = '{}'.format(time.time())
        filename = os.path.join(output_dir, '{}.png'.format(num_plots))
        render_plot(plot_conf, filename)
        num_plots += 1
        rendered_q.put(dict(
            filename=filename,
            caption=caption
        ))
    shutil.rmtree(output_dir, ignore_errors=True)  # clean up those files!


def render_plot(plot_conf, filename='/tmp/test.png'):
    import matplotlib.pyplot as plt
    # TODO: learn matlabplot
    plt.clf()
    values = plot_conf.pop('values')
    plt.fill_between(xrange(len(values)), values)
    for key, value in plot_conf.items():
        getattr(plt, key)(value)  # it's a bunch of callables?
    plt.savefig(filename)


def configure_plots(buffer_reader, plot_q):
    """Transform the sample buffer into some plotting commands."""
    while True:
        values, top_tweet_url = buffer_reader.get()
        caption = time.strftime('%I:%M %p %Z on %a, %x')
        if len(values):
            avg = sum(values) / len(values)
            stat_info = 'Average {0:.2f} LOL/s'.format(avg)
            caption = '{} at {}'.format(stat_info, caption)
        if top_tweet_url:
            caption += ' Top tweet: {}'.format(top_tweet_url) 
        conf = dict(
            values=values,
            caption=caption,
            xticks=[]
        )
        plot_q.put(conf)


def tweet_rendered(twitter_api, rendered_q):
    for item in rendered_q:
        if twitter_api and not DEBUG:
            twitter_api.PostMedia(
                item['caption'], item['filename'])
            os.unlink(item['filename'])
        else:
            print(unicode(item))


def renderer_process(buffer_reader, output_dir='/tmp/'):
    plot_q = Queue()
    rendered_q = Queue()
    try:
        twitter_api = get_twitter_api()
    except KeyError:
        twitter_api = None
    confs = [
        [configure_plots, buffer_reader, plot_q],
        [plotter, plot_q, rendered_q, output_dir],
        [tweet_rendered, twitter_api, rendered_q],
    ]
    if not DEBUG:
        for conf in confs:
            conf[0] = never_surrender(conf[0])
    spawn_greenlets(confs)


def main():
    try:
        buffer_reader, buffer_writer = gipc.pipe()

        processes = spawn_processes([
            (sampler_process, buffer_writer, sample_counter()),
            (renderer_process, buffer_reader,),
        ])
        while True:
            gevent.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
    