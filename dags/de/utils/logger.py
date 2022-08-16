import os
import sys
import logging
import logging.handlers

from de.utils.common import AppInfo
from logging import INFO, WARNING, ERROR, DEBUG


# log level 추가시 선언
# DEBUG -> 10, DETAIL -> 18, MORE -> 19, INFO -> 20, WARNING -> 30
MORE = 19
DETAIL = 18

# logger
get_logger = logging.getLogger
app_name = AppInfo().app_name()  # 현재 실행되는 모듈의 app_name 추출

# log level 추가
logging.addLevelName(MORE, "MORE")
logging.addLevelName(DETAIL, "DETAIL")

# log 출력 메소드 추가
logging.Logger.more = lambda inst, msg, *args, **kwargs: inst.log(MORE,
                                                                  f"[{get_caller()}] {msg}",
                                                                  *args,
                                                                  **kwargs)
logging.Logger.detail = lambda inst, msg, *args, **kwargs: inst.log(DETAIL,
                                                                    f"[{get_caller(2)}>{get_caller()}] {msg}",
                                                                    *args,
                                                                    **kwargs)
logging.Logger.logs = lambda inst, level, msg, *args, **kwargs: inst.log(level,
                                                                         f"[{get_caller(1, False)}] {msg}",
                                                                         *args,
                                                                         **kwargs)
logging.Logger.logc = lambda inst, level, msg, *args, **kwargs: inst.log(level,
                                                                         f"[{get_caller()}] {msg}",
                                                                         *args,
                                                                         **kwargs)
logging.Logger.logcc = lambda inst, level, msg, *args, **kwargs: inst.log(level,
                                                                          f"[{get_caller(2)}>{get_caller()}] {msg}",
                                                                          *args,
                                                                          **kwargs)


def get_caller(step=1, fullname=True):
    """
        A function that returns the caller(function) of the code at the executed location.
    :param step:
        caller depth.
    :param fullname:
        True if you want to return the full_path_name
            containing the caller(function) path, False otherwise
    :return:
    """
    step += 1
    caller = sys._getframe(step).f_locals.get('self')
    if isinstance(caller, type(None)):
        return sys._getframe(step).f_code.co_name
    elif fullname:
        return str(sys._getframe(step).f_locals.get('self')).split(" ")[0][1:] + "." + \
               sys._getframe(step).f_code.co_name
    else:
        return sys._getframe(step).f_code_co_name


def caller_id():
    """
    return caller class id
    """
    return id(sys._getframe(2).f_locals.get('self'))


def set_logging_level(logger_name=None, logging_level=None):
    """
    set or change logging level
    """
    if logging_level:
        if isinstance(logging_level, str):
            logging_level = logging_level.upper()
        c_logger = logging.getLogger(logger_name)
        c_logger.setLevel(logging_level)


_stream_handler_enabled = []  # logger 의 스트림 여부
_file_handlers = []           # logger 의 파일 로깅 여부
_default_format = '%(asctime)s %(levelname)s %(message)s'


def create_logger(log_file, logger_name=None, err_logfile=True, max_bytes=104857600,
                  back_up_count=3, propagate=True, format=_default_format, stream_enabled=True):
    """
    logger 생성.
    default (log file size 100MB, log format --> 'YYYY-MM-DD hh:mm:ss:ms loglevel message'
    :param log_file:
    :param logger_name: 생성할 logger name
    :param err_logfile: err log file 에 logging 여부
    :param max_bytes: default 100MB
    :param back_up_count: rotateFileHandler 최대 backup file 개수
    :param propagate: 상위 logger 전파 여부
    :param format: logging default format 상단 참조
    :param stream_enabled:
    :return:
    """
    global _stream_handler_enabled
    global _file_handlers

    clogger = logging.getLogger(logger_name)
    clogger.setLevel(INFO)
    clogger.propagate = propagate  # 상위 logger 전파 여부
    formatter = logging.Formatter(format)

    # 핸들러 중복 선언 방지
    # ** stream_handler --> console 에 메세지를 전달
    if stream_enabled and logger_name not in _stream_handler_enabled:
        _stream_handler_enabled.append(logger_name)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        clogger.addHandler(stream_handler)

    # log 경로 생성

    # os.path.dirname --> 경로 중 directory 명만 얻기
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)  # exist_ok --> 해당 디렉토리가 있으면 무시

    # ** file_handler --> file 에 메세지를 전달
    # RotatingFileHandler -> 필요한 갯수 만큼 백업 file 을 생성하여 관리
    # .log / .err file 을 생성.
    #  - .log --> 사용자가 app_properties.yml 에서 설정한 logger level 에 따라 logger level 이 설정
    if log_file not in _file_handlers:
        _file_handlers.append(log_file)
        file_handler = logging.handlers.RotatingFileHandler(log_file,
                                                            maxBytes=max_bytes,
                                                            backupCount=back_up_count)
        file_handler.setFormatter(formatter)
        clogger.addHandler(file_handler)

        # .err -> log 중 ERROR 라고 지정한 것들만 따로 저장하기 위하여 .err file 지정
        if err_logfile:
            err_file_handler = logging.handlers.RotatingFileHandler(log_file.replace(".log", ".err"),
                                                                    maxBytes=max_bytes,
                                                                    backupCount=back_up_count)
            err_file_handler.setFormatter(formatter)
            err_file_handler.setLevel(ERROR)
            clogger.addHandler(err_file_handler)

    return clogger
