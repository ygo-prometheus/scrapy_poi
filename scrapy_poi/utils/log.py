#!/usr/bin/env python3
# coding=utf-8
import os
import logging
import logging.handlers

__all__ = ['init_logger', 'Is_Stream_Handler']


def init_logger(name, log_file_name='', log_dir='./log/', level=logging.DEBUG, is_stream_handler=True):
    _logger = logging.getLogger(name)
    # _logger.setLevel(logging.DEBUG)
    _logger.setLevel(logging.INFO)

    if log_file_name:
        log_dir = os.path.expanduser(log_dir)
        if not os.path.exists(log_dir) or not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.path.join(log_dir, '{}.log'.format(log_file_name)), when='D', encoding='utf-8'
        )
        file_handler.setLevel(level)
        # file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
        )

        _logger.addHandler(file_handler)

    # 到屏幕
    if is_stream_handler:
        console = logging.StreamHandler()
        console.setLevel(level)
        # 设置日志打印格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        _logger.addHandler(console)

    return _logger


if __name__ == '__main__':
    log = init_logger('test')
    log.info('hehhe')