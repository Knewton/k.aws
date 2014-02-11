# This fix is so that you can reference modules in your local directory that
# have k. as a parent while also referencing other modules that have k. as a
# parent that are installed on your system/virtualenv
import warnings
warnings.filterwarnings('ignore', '.*Module k was already imported from.*', UserWarning)
try:
	__import__('pkg_resources').declare_namespace(__name__)
except ImportError:
	from pkgutil import extend_path
	__path__ = extend_path(__path__, __name__)
