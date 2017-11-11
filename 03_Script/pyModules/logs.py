import os, colorlog, logging, time

#def remove_LOG_File(filename):
#
#    try:
#        os.remove(filename)
#    except OSError:
#        print 'error deleting log file'
#        pass


def get_time ():
    return time.time()

def get_total_time (start, end):
    return round((end - start) / 60, 2)

def initialize_Log(fileName, mode):

    global logger
    global handler
    global file

    cwd = os.getcwd()

    folder = cwd + '/logs/'

    if not os.path.exists(folder):
        os.makedirs(folder)

    file = folder + fileName + '.log'

    # print file

#    remove_LOG_File(file)


    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)s:%(name)s:%(message)s'))

    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%H:%M:%S',
                    filename=file,
                    filemode=mode)

    logger = logging.getLogger(fileName)

    logger = colorlog.getLogger(fileName)
    logger.addHandler(handler)

    logger.warning('is when this event was logged.')


def info(information):

    logger.info(information)

def warning(information):

    logger.warning(information)

def debug(information):

    logger.debug(information)

def error(information):

    logger.error(information)

def critical(information):

    logger.critical(information)

def log_close():

    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)

    logging.shutdown()
    handler.close()
    logger.removeHandler(handler)
