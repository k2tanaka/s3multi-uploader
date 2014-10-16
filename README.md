## about

指定したローカルディレクトリ配下の全ファイルを再帰的にS3にアップロードします

## sample

python s3uploader.py -a "aws access_key" -s "aws secret_key" -b "s3 bucket_name" -f "local_dir" -t "s3 start_dir" -p "multi process count"

## option

``` -a: aws access_key ```
``` -s: aws secret_key ```
``` -b: s3 bucket name ```
``` -f: local directory ```
``` -t: remote(s3) directory ```
``` -p: upload process count ```
``` -c: s3 cache controle(sec) ```

