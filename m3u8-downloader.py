import requests
import re
from Crypto.Cipher import AES
from queue import Queue
import threading
import time
import datetime
import os
import sys
from urllib.parse import urljoin


class MediaPart:
    def __init__(self, url, time_len, cryptor, header):
        self.url = url
        self.filename = url.split('/')[-1].replace('+', '_')
        if len(self.filename) < 3 or self.filename[-3:].lower() != '.ts':
            self.filename += '.ts'
        self.time_len = time_len
        self.cryptor = cryptor
        self.header = header
        self.downloaded = False
        self.data_len = 0
        self.retry_times = 0

    def rename(self):
        self.filename = '{0}_0.ts'.format(self.filename[:-3])


class M3U8File:
    def __init__(self, file_or_url, base_url, target_filename, cache='cache/', skip_discontinuity=False, key_path=None):
        self.file_or_url = file_or_url
        self.target_filename = target_filename
        self.cache = cache
        self.skip_discontinuity = skip_discontinuity
        self.key_path = key_path
        self.base_url = base_url
        self.download_headers = {}
        self.key_headers = {}
        self.media_parts = []
        self.total_time = 0
        self.skiped_total_time = 0
        self.skiped_count = 0

    def download(self, merge=True):
        # noinspection PyBroadException
        try:
            m3u8_content = self._read_file(self.file_or_url)
        except Exception as _:
            print('下载失败: 无法下载文件')
            return

        # 查看文件是否时 m3u8 格式
        text_lines = m3u8_content.split('\n')
        if text_lines[0].upper() != '#EXTM3U':
            print('下载失败: m3u8 格式错误')
        text_lines = text_lines[1:]

        self._parse_m3u8(text_lines)
        self._multiprocess_download()
        filelist = self._make_filelist()
        if merge:
            self._merge_file(filelist)
            self._remove_cache()

    # noinspection PyMethodMayBeStatic
    def _read_from_file(self, file):
        try:
            with open(file, 'r') as f:
                content = f.read()
                return content
        except Exception as _:
            raise FileNotFoundError()

    # noinspection PyMethodMayBeStatic
    def _read_from_url(self, url):
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                return resp.text
        except Exception as _:
            raise FileNotFoundError()

    def _read_file(self, file_or_url):
        if file_or_url[:7].lower() == 'http://' or file_or_url[:8] == 'https://':
            self.base_url = os.path.dirname(file_or_url) + '/'
            return self._read_from_url(file_or_url)
        else:
            return self._read_from_file(file_or_url)

    def _get_key_from_url(self, key_url):
        resp = requests.get(key_url, headers=self.key_headers)
        key = resp.content
        return key

    def _get_key_from_file(self, key_name):
        if self.key_path is not None:
            key_path = os.path.dirname(self.key_path)
        else:
            key_path = os.path.dirname(self.file_or_url)

        key_path = os.path.join(key_path, key_name)
        try:
            with open(key_path, 'rb') as f:
                key = f.read()
        except Exception as _:
            raise FileNotFoundError()
        return key

    def _get_key(self, key_intro):
        params = key_intro.split(',')
        method = ''
        key_url = ''
        for p in params:
            kv = p.split('=')
            if kv[0].strip().upper() == 'METHOD':
                method = kv[1].strip()
            if kv[0].strip().upper() == 'URI':
                key_url = re.search('"(.*?[\\w]*.key)"', kv[1]).group(1).strip()
        if len(key_url) > 0:
            if self.key_path is not None:
                key = self._get_key_from_file(key_url)
            else:
                if key_url[:7].lower() != 'http://' and key_url[:8].lower() != 'https://' and \
                   self.base_url is not None and len(self.base_url) > 0:
                    key_url = urljoin(self.base_url, key_url)
                if key_url[:7].lower() == 'http://' or key_url[:8].lower() == 'https://':
                    key = self._get_key_from_url(key_url)
                else:
                    key_url = urljoin(os.path.dirname(self.file_or_url), key_url)
                    if key_url[:7].lower() == 'http://' or key_url[:8].lower() == 'https://':
                        key = self._get_key_from_url(key_url)
                    else:
                        key = self._get_key_from_file(key_url)
            if method.upper() == 'AES-128':
                cryptor = AES.new(key, AES.MODE_CBC, key)
            else:
                raise NotImplementedError('method [{0}] not implemented.'.format(method))
            return cryptor
        else:
            return None

    def _parse_x_map(self, x_map):
        params = x_map.split(',')
        byte_range = None
        key_url = None
        for p in params:
            kv = p.split('=')
            if kv[0].strip().upper() == 'URI':
                key_url = kv[1].strip()
                if key_url[0] == '"':
                    key_url = key_url[1:-1]
            if kv[0].strip().upper() == 'BYTERANGE':
                byte_range = kv[1].strip()
                if byte_range[0] == '"':
                    byte_range = byte_range[1:-1]
        if len(key_url) > 0:
            if key_url[:7].lower() != 'http://' and key_url[:8].lower() != 'https://' and \
               self.base_url is not None and len(self.base_url) > 0:
                key_url = urljoin(self.base_url, key_url)
            if key_url[:7].lower() == 'http://' or key_url[:8].lower() == 'https://':
                key = self._get_key_from_url(key_url)
            else:
                key_url = urljoin(os.path.dirname(self.file_or_url), key_url)
                if key_url[:7].lower() == 'http://' or key_url[:8].lower() == 'https://':
                    key = self._get_key_from_url(key_url)
                else:
                    key = self._get_key_from_file(key_url)
            if byte_range is not None:
                byte_range = byte_range.split('@')
                start_pos = int(byte_range[0].strip())
                end_pos = int(byte_range[1].strip())
                key = key[start_pos:end_pos]
            return key
        else:
            return None

    # noinspection PyMethodMayBeStatic
    def _format_time(self, time_len):
        s = ''
        time_len = int(time_len)
        if time_len > 60 * 60:
            s += '{0} 小时 '.format(int(time_len / 60 / 60))
            time_len = time_len % (60 * 60)

        if time_len > 60:
            s += '{0} 分钟 '.format(int(time_len / 60))
            time_len = time_len % 60
        elif len(s) > 0:
            s += '0 分钟 '

        s += '{0} 秒'.format(int(time_len))
        return s

    def _rename_repeat_name(self):
        names = []
        for mp in self.media_parts:
            while mp.filename in names:
                mp.rename()
            names.append(mp.filename)

    def _parse_m3u8(self, text_lines):
        self.media_parts.clear()
        self.total_time = 0
        self.skiped_count = 0
        self.skiped_total_time = 0

        current_key = None
        x_map = None
        time_len = 0
        skip = False
        is_next_video_path = False

        for line in text_lines:
            if len(line) == 0:
                continue
            elif line[0] == '#':
                sec = line[1:].split(':')
                xtitle = sec[0].strip().upper()
                if xtitle == 'EXTM3U':
                    pass
                elif xtitle == 'EXT-X-ALLOW-CACHE':
                    pass
                elif xtitle == 'EXT-X-VERSION':
                    print("version: ", sec[1].strip())
                elif xtitle == 'EXT-X-TARGETDURATION':
                    print("target duration: ", sec[1].strip())
                elif xtitle == 'EXT-X-PLAYLIST-TYPE':
                    print("playlist type: ", sec[1].strip())
                elif xtitle == 'EXT-X-MEDIA-SEQUENCE':
                    print("media sequence: ", sec[1].strip())
                elif xtitle == 'EXT-X-KEY':
                    current_key = self._get_key(':'.join(sec[1:]).strip())
                elif xtitle == 'EXTINF':
                    time_len = float(sec[1].split(',')[0])
                    is_next_video_path = True
                elif xtitle == 'EXT-X-DISCONTINUITY':
                    if self.skip_discontinuity:
                        if not skip:
                            skip = True
                        else:
                            skip = False
                elif xtitle == 'EXT-X-ENDLIST':
                    break
                elif xtitle == 'EXT-X-MAP':
                    x_map = self._parse_x_map(sec[1].strip())
                else:
                    print('unknown line: {0}'.format(line))
            elif is_next_video_path:
                is_next_video_path = False
                if skip:
                    self.skiped_count += 1
                    self.skiped_total_time += time_len
                else:
                    self.total_time += time_len
                    if len(line) < 7 or (line[:7].lower() != 'http://' and line[:8].lower() != 'https://'):
                        if self.base_url is None or len(self.base_url) == 0:
                            raise FileNotFoundError('请使用 -b 指定视频文件的链接')
                        line = urljoin(self.base_url, line)
                    self.media_parts.append(MediaPart(line, time_len, current_key, x_map))
            else:
                print('[warning] 解析文件，未知内容: {0}'.format(line))

        self._rename_repeat_name()
        intro = '解析完成，共 {0} 个段落，时长 {1}'.format(len(self.media_parts), self._format_time(self.total_time))
        if self.skip_discontinuity and self.skiped_count > 0:
            intro += '，跳过 {0}'.format(self._format_time(self.skiped_total_time))
        print(intro)

    def _multiprocess_download(self, retrying=False):
        mp_count = 0
        ts_queue = Queue(len(self.media_parts))
        for mp in self.media_parts:
            if mp.downloaded:
                continue
            mp_count += 1
            ts_queue.put(mp)

        if mp_count == 0:
            return

        if retrying:
            will_retry = input('还有 {0} 个文件未下载，是否重试？ [Y/n]'.format(mp_count))
            if will_retry.lower() != 'y':
                return

        print('开始下载')

        start = datetime.datetime.now().replace(microsecond=0)

        # 最大下载线程小于 50 个
        thread_count = 1
        if mp_count > 5:
            thread_count = mp_count // 5

        if thread_count > 50:
            thread_count = 50

        print('线程数: {0}'.format(thread_count))

        threads = []
        for i in range(thread_count):
            t = threading.Thread(
                target=self._download_thread,
                name='th-' + str(i),
                kwargs={'ts_queue': ts_queue, 'headers': self.download_headers, 'cache': self.cache}
            )
            t.setDaemon(True)
            threads.append(t)
        for t in threads:
            time.sleep(0.4)
            t.start()
        for t in threads:
            t.join()

        print('\n')
        print("下载任务结束")
        end = datetime.datetime.now().replace(microsecond=0)
        print('写文件及下载耗时：' + str(end - start))

        # 重试未下载的段落
        self._multiprocess_download(True)

    @staticmethod
    def _download_thread(ts_queue, headers, cache):
        while not ts_queue.empty():
            mp = ts_queue.get()
            savepath = os.path.join(cache, mp.filename)

            # noinspection PyBroadException
            try:
                # noinspection PyUnresolvedReferences
                requests.packages.urllib3.disable_warnings()
                resp = requests.get(mp.url, headers=headers)
                if resp.status_code != 200:
                    raise FileNotFoundError()
                with open(savepath, 'wb') as f:
                    if mp.cryptor is not None:
                        data = mp.cryptor.decrypt(resp.content)
                    else:
                        data = resp.content
                    if mp.header is not None:
                        f.write(mp.header)
                    f.write(data)
                    mp.downloaded = True
                    mp.data_len = len(data)
                print("\r", '任务文件 {0} 下载成功 剩余: {1}'.format(mp.filename, ts_queue.qsize()), end='', flush=True)
            except Exception as _:
                mp.retry_times += 1
                if mp.retry_times > 10:
                    print("\n", '任务文件 {0} 下载失败, 已经尝试了 {1} 次, 跳过'.format(mp.filename, mp.retry_times))
                else:
                    # print("\n", '任务文件 ', mp.filename, ' 下载失败, 重试第 {0} 次'.format(mp.retry_times))
                    ts_queue.put(mp)

    def _make_filelist(self):
        filelist = os.path.join(self.cache, 'filelist.txt')
        with open(filelist, 'w') as f:
            for mp in self.media_parts:
                if not mp.downloaded:
                    continue
                f.write('file {0}\n'.format(mp.filename))
        return filelist

    def _merge_file(self, filelist):
        print('')
        print('开始合并视频')
        start = datetime.datetime.now().replace(microsecond=0)

        # noinspection PyBroadException
        try:
            command = 'ffmpeg '
            command += ' -y -f concat -i %s -bsf:a aac_adtstoasc -c copy %s' % (filelist, self.target_filename)
            os.system(command)
            print('视频合并完成')
        except Exception as _:
            print('视频合并失败')
            return

        end = datetime.datetime.now().replace(microsecond=0)
        print('视频合并耗时：' + str(end - start))

    def _remove_cache(self):
        print('')
        print('开始清除缓存')
        for mp in self.media_parts:
            try:
                os.remove(os.path.join(self.cache, mp.filename))
            except Exception as _:
                print('删除文件 {0} 失败，skip'.format(mp.filename))
        print("ts 文件全部删除")

        # noinspection PyBroadException
        try:
            os.remove(os.path.join(self.cache, 'filelist.txt'))
            print('文件删除成功')
        except Exception as _:
            print('文件删除失败')


def usage():
    print(os.path.basename(sys.argv[0]), '多线程下载 M3U8 并合并文件')
    print('')
    print('usage: {0} -i m3u8file [-c cache] [-p] [-k key file path] -o outfile'.format(os.path.basename(sys.argv[0])))
    print('')
    print('basic options:')
    print('-i\t\t\tM3U8 文件地址')
    print('-c\t\t\t缓存地址，默认：\'cache/\'')
    print('-o\t\t\t输出文件名')
    print('')
    print('advance options:')
    print('--no-merge\t\t\t只下载，不合并视频')
    print('-b\t\t\t指定下载片段的网址')
    print('-p\t\t\t跳过非连续片段(非连续片段可能是广告)')
    print('-k\t\t\tkey 文件地址')


if __name__ == '__main__':
    if len(sys.argv) == 1:
        usage()
        exit(1)

    video_base_url = None
    m3u8_url = None
    output_file = None
    cache_path = 'cache/'
    sec_key_path = None
    skip_disc = False
    no_merge = False

    arg_index = 1
    while arg_index < len(sys.argv):
        if sys.argv[arg_index].lower() == '-i' and len(sys.argv) > (arg_index+1):
            m3u8_url = sys.argv[arg_index+1]
            arg_index += 2
        elif sys.argv[arg_index].lower() == '-c' and len(sys.argv) > (arg_index+1):
            cache_path = sys.argv[arg_index+1]
            arg_index += 2
        elif sys.argv[arg_index].lower() == '-o' and len(sys.argv) > (arg_index+1):
            output_file = sys.argv[arg_index+1]
            arg_index += 2
        elif sys.argv[arg_index].lower() == '-p':
            skip_disc = True
            arg_index += 1
        elif sys.argv[arg_index].lower() == '-k' and len(sys.argv) > (arg_index+1):
            sec_key_path = sys.argv[arg_index+1]
            arg_index += 2
        elif sys.argv[arg_index].lower() == '--no-merge':
            no_merge = True
            arg_index += 1
        elif sys.argv[arg_index].lower() == '-b' and len(sys.argv) > (arg_index+1):
            video_base_url = sys.argv[arg_index+1]
            arg_index += 2
        else:
            arg_index += 1

    if m3u8_url is None or \
       ((not no_merge) and output_file is None) or \
       (no_merge and output_file is not None):
        usage()
        exit(1)

    if not os.path.exists(cache_path):
        os.mkdir(cache_path)

    M3U8File(m3u8_url, video_base_url, output_file, cache_path, skip_disc, sec_key_path).download(not no_merge)
