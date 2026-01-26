from datetime import datetime
import logging
import os

"""
Utility module for setting up loggers:
Provides a function to create and configure loggers with specified names and log levels.
"""

def get_logger(name: str = 'logger', log_level: str = 'INFO') -> logging.Logger:
    """
    Creates and returns a logger with the specified name and log level.
    Args:
        log_level (str): Logging level as a string (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    
    # Convert log level string to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Prevent propagation to root logger (avoids duplicate messages from Airflow)
    logger.propagate = False

    # Configure handlers if not already configured
    if not logger.handlers:
        # Formatter for both handlers
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s [%(name)s] '
            '%(filename)s:%(lineno)d %(funcName)s() - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler with UTF-8 encoding to support Unicode characters
        # Generate hourly log files app.log + timestamp for example app_2023-10-05_14_.log
        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)  # Create logs directory if it doesn't exist
        log_file_name = f'{log_dir}/pdm2ec_log_{datetime.now().strftime("%Y-%m-%d_%H")}.log'
        file_handler = logging.FileHandler(log_file_name, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
