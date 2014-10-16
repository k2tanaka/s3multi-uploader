#!/usr/bin/python
# coding: utf8

# python s3uploader.py -a <aws access_key> -s <aws secret_key> b <s3 bucket_name> -f <local_dir> -t <s3 start_dir> -p 8

import sys
import os
import multiprocessing

from multiprocessing import Pool
from optparse import OptionParser
from boto.s3.connection import S3Connection


def parse_option():
    # getopts
    optp = OptionParser()

    optp.add_option(
        "-a", "--access_key", dest="access_key", help=u"AWS access key")
    optp.add_option(
        "-s", "--secret_key", dest="secret_key", help=u"AWS シークレッキー")
    optp.add_option(
        "-b", "--bucket_name", dest="bucket_name", help=u"S3 バケット名")
    optp.add_option(
        "-f", "--from", dest="src",
        help=u"アップロードする基点となるディレクトリのフルパス")
    optp.add_option(
        "-t", "--to", dest="dest",
        help=u"S3 アップロード先のパス Bucket名は除く、基点となる \
            S3ディレクトリ名。バケット直下に配置する場合は'/'指定")

    # 以下は任意オプション
    optp.add_option(
        "-c", "--cache_control", dest="cache", help=u"cache 保存期間(sec)")
    optp.add_option(
        "-p", "--process", dest="process", help=u"multi process 並列度")

    (opts, args) = optp.parse_args()

    # args check
    messages = []
    if not opts.access_key:
        messages.append("-a/--access_key options is required.")
    if not opts.secret_key:
        messages.append("-s/--secret_key options is required.")
    if not opts.bucket_name:
        messages.append("-b/--bucket_name options is required.")
    if not opts.src:
        messages.append("-f/--from options is required.")
    if not opts.dest:
        messages.append("-t/--to options is required.")

    # set default
    if not opts.process:
        opts.process = multiprocessing.cpu_count()

    if 0 < len(messages):
        print >>sys.stderr,  "[ERROR] command options error.\n"
        for message in messages:
            print >>sys.stderr, "\t" + message + "\n"
            sys.exit(2)

    return (opts, args)


def setup():
    # parse option
    (opts, args) = parse_option()

    # S3 connect
    conn = S3Connection(
        aws_access_key_id=opts.access_key,
        aws_secret_access_key=opts.secret_key)

    bucket = conn.get_bucket(opts.bucket_name)

    return (opts, args, bucket)


def get_key_name(fullpath, path, start_point_s3):
    # 対象ファイルパスからローカルパスを削除し、S3基点ディレクトリを
    # 付加したものがobject_keyとなる
    # example) local: /tmp/contents, s3dir: contents/body
    # fullpath: /tmp/contents/img/a.png => contents/body/img/a.png

    if start_point_s3 == '/':
        key_name = fullpath.replace(path, '')
    else:
        key_name = start_point_s3 + fullpath.replace(path, '')

    l = key_name.split(os.sep)
    return '/'.join(l)


def upload(opts, bucket):
    """指定されたローカルディレクトリ配下の全ファイルを、対象バケット基点オブジェクト配下にアップロード
    """

    pool = None
    try:
        path = opts.src
        start_point_s3 = opts.dest
        cache_control = opts.cache

        pool = Pool(processes=int(opts.process))

        for root, dirs, files in os.walk(path):
            for file in files:

                upload_file = os.path.join(root, file)
                key_name = get_key_name(upload_file, path, start_point_s3)

                k = bucket.new_key(key_name)
                # S3オブジェクトのMetaData(cache_control)設定
                # 値が0の場合は、cacheなし設定
                if cache_control is not None:
                    if int(cache_control) > 0:
                        v = 'max-age=%s, public' % (cache_control)
                        k.set_metadata('Cache-Control', v)

                    else:
                        k.set_metadata(
                            'Cache-Control',
                            'private,no-store,no-cache,max-age=0')
                        k.set_metadata('Expires', '-1')

                # workerでアップロード実行
                pool.apply_async(
                    worker,
                    [k, upload_file], callback=multiple_return)

        # worker error handling
        if 1 in return_list:
            raise Exception("worker error")

    except Exception, e:
        print "upload() error - bucket=%s ¥n%s" % (opts.bucket_name, str(e))
        sys.exit(1)

    finally:
        if pool is not None:
            pool.close()
            pool.join()


return_list = []


def multiple_return(ret):
    return_list.append(ret)


def worker(key, upload_file):
    try:
        key.set_contents_from_filename(upload_file)
        return 0

    except Exception, e:
        print "worker() error ¥n%s" % (str(e))
        return 1


print 'start ================================'
# setup
(opts, args, bucket) = setup()
upload(opts, bucket)
print 'end ================================'
