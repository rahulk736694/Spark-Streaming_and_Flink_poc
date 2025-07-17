from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class LogicalTypes(object):

  class Enum(object):
    PYTHON_CALLABLE = PropertiesFromEnumValue('beam:logical_type:python_callable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MICROS_INSTANT = PropertiesFromEnumValue('beam:logical_type:micros_instant:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MILLIS_INSTANT = PropertiesFromEnumValue('beam:logical_type:millis_instant:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DECIMAL = PropertiesFromEnumValue('beam:logical_type:decimal:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

