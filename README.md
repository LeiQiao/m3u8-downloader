# m3u8-downloader
multi-threading download m3u8 files and using `ffmpeg` convert to single video file

## usage
```shell
m3u8-downloader.py -i m3u8file [-c cache] [-k] -o outfile

basic options:
-i			M3U8 input (url or file)
-c			cache path default: 'cache/'
-o		    output file

advance options:
-k			skip discontinuity (more like advertise)

Process finished with exit code 1
```

for example

```shell
python3 m3u8-downloader.py -i teacher_cang.m3u8 -c cache -k -o teacher_cang.mp4
```
