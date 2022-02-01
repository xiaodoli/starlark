load("json.star", "json")
load("logging.star", "log")
load("time.star", "time")

def apply(metric):
  jobj = json.decode(metric.fields.get("value"))
  # log.debug("json_obj={}".format(jobj))

  # name is type.ibx
  measurement = jobj["type"]
  ibx = jobj["data"]["ibx"]
  new_metric = Metric(measurement)
  new_metric.tags["type"] = measurement
  eventName = jobj["type"] + "-" + ibx
  new_metric.tags["ProviderName"] = "EQIX"
  new_metric.tags["eventName"] = eventName
  new_metric.tags["source"] = "events"
  new_metric.fields["ReceivedTime"] = float(time.now().unix_nano)
  new_metric.fields["IsIntegrity"] = float(0)
  new_metric.fields["EventStatus"] = float(131072)

  tagsToAdd = ["type", "messageRetried"]
  for tagName in tagsToAdd:
    addTag(jobj, "", tagName, new_metric)

  if measurement == "tag-point":
    readTagPoint(jobj, new_metric)
  elif measurement == "environmental":
    readEnvironment(jobj, new_metric)
  elif measurement == "system-alert":
    readSystemAlert(jobj, new_metric)
  elif measurement == "custom-alert":
    readCustomAlert(jobj, new_metric)
  elif measurement == "power":
    readPower(jobj, new_metric)

  # log.debug("new metric formatted={}".format(new_metric))
  return new_metric

def readTagPoint(jobj, metric):
  data = jobj["data"]
  if fieldExists(data, "reading"):
    readingObj = data["reading"]
    readReading(readingObj, metric)
  addTime(data, "readingTime", metric)
  if fieldExists(data, "tag"):
    tagObj = data["tag"]
    readTag(tagObj, metric)
  tagsToAdd = ["dataQuality"]
  for tagName in tagsToAdd:
    addTag(jobj, "", tagName, metric)


def readEnvironment(jobj, metric):
  data = jobj["data"]
  if fieldExists(data, "reading"):
    readingObj = data["reading"]
    readReading(readingObj, metric)
  addTime(data, "readingTime", metric)
  if fieldExists(data, "tag"):
    tagObj = data["tag"]
    readTag(tagObj, metric)
  if fieldExists(data, "asset"):
    assetObj = data["asset"]
    readAsset(assetObj, metric)
  tagsToAdd = ["dataQuality"]
  for tagName in tagsToAdd:
    addTag(jobj, "", tagName, metric)


def readSystemAlert(jobj, metric):
  data = jobj["data"]
  if fieldExists(data, "currentValue"):
    currentValue = data["currentValue"]
    readReading(currentValue, metric)
  addTime(data, "readingTime", metric)
  if fieldExists(data, "tag"):
    tagObj = data["tag"]
    readTag(tagObj, metric)
  if fieldExists(data, "asset"):
    assetObj = data["asset"]
    readAsset(assetObj, metric)
  if fieldExists(data, "threshold"):
    threadObj = data["threshold"]
    readThreshold(threadObj, metric)
  stringFields = ["conditionName", "status", "triggerRule", "definitionId"]
  for fieldName in stringFields:
    addFieldString(data, "", fieldName, metric)
  stringFields2 = ["type"]
  for fieldName in stringFields2:
    addFieldString(data, "system-alert-", fieldName, metric)
  timeFields = ["triggeredTime", "processedTime", "normalTriggeredTime", "normalProcessedTime"]
  for timeField in timeFields:
    addFieldDate(data, "", timeField, metric)
  intFields = ["severity"]
  for intField in intFields:
    addFieldInt(data, "", intField, metric)
  boolFields = ["heartbeat"]
  for boolField in boolFields:
    addFieldBool(data, "", boolField, metric)


def readCustomAlert(jobj, metric):
  data = jobj["data"]
  if fieldExists(data, "asset"):
    assetObj = data["asset"]
    readAsset(assetObj, metric)
  if fieldExists(data, "tag"):
    tagObj = data["tag"]
    readTag(tagObj, metric)
  if fieldExists(data, "threshold"):
    threadObj = data["threshold"]
    readThreshold(threadObj, metric)
  stringFields = ["region", "id", "typeId", "conditional", "eventType"]
  for fieldName in stringFields:
    addFieldString(data, "", fieldName, metric)
  stringFields2 = ["type"]
  for fieldName in stringFields2:
    addFieldString(data, "custom-alert-", fieldName, metric)
  timeFields = ["triggeredTime"]
  for timeField in timeFields:
    addFieldDate(data, "", timeField, metric)
  boolFields = ["heartbeat"]
  for boolField in boolFields:
    addFieldBool(data, "", boolField, metric)


def readPower(jobj, metric):
  data = jobj["data"]
  powerReadingNames = ["realPower", "apparentPower", "current", "powerFactor", "soldCurrent", "soldPower", "powerConsumptionToContractual"]
  for powerReadingName in powerReadingNames:
    if fieldExists(data, powerReadingName):
      powerReadingObj = data[powerReadingName]
      powerReadingPrefix = powerReadingName + "-"
      readPowerReading(powerReadingObj, powerReadingPrefix, metric)
  addTime(data, "readingTime", metric)
  if fieldExists(data, "asset"):
    assetObj = data["asset"]
    readAsset(assetObj, metric)
  if fieldExists(data, "tag"):
    tagObj = data["tag"]
    readTag(tagObj, metric)
  stringFields = ["cage", "cabinet", "accountNumber", "peakLastSevenDaysTime", "oid", "circuitType", "customerName"]
  for fieldName in stringFields:
    addFieldString(data, "", fieldName, metric)
  timeFields = ["lastUpdated"]
  for timeField in timeFields:
    addFieldDate(data, "", timeField, metric)


def readTag(tagObj, metric):
  tagsToAdd = ["unit", "value", "type"]
  for tagName in tagsToAdd:
    addTag(tagObj, "tag-", tagName, metric)

def readReading(readingObj, metric):
  if fieldExists(readingObj, "value"):
    value = readingObj["value"]
    metric.fields["Value"] = str(value)
  tagsToAdd = ["unit", "type"]
  for tagName in tagsToAdd:
    addTag(readingObj, "reading-", tagName, metric)


def readPowerReading(powerReadingObj, prefix, metric):
  strFields = ["unit", "value"]
  for strField in strFields:
    addFieldString(powerReadingObj, prefix, strField, metric)

def readPowerValue(dataJobj, field, metric):
  valueObj = dataJobj[field]
  if valueObj != None:
    metric.fields[field] = float(valueObj["value"])
    metric.tags[field + "Unit"] = str(valueObj["unit"])

def readAsset(assetObj, metric):
  tagsToAdd = ["id", "type", "level", "classification"]
  for tagName in tagsToAdd:
    addTag(assetObj, "asset-", tagName, metric)


def readThreshold(thresholdObj, metric):
  stringFields = ["unit", "value", "stateLimit", "message"]
  for strField in stringFields:
    addFieldString(thresholdObj, "threshold-", strField, metric)


def addTag(entry, prefix, field, metric):
  if fieldExists(entry, field):
    fieldName = prefix + field
    metric.tags[fieldName] = str(entry[field])


def addFieldInt(entry, prefix, field, metric):
  if fieldExists(entry, field):
    fieldName = prefix + field
    metric.fields[fieldName] = int(entry[field])


def addFieldFloat(entry, prefix, field, metric):
  if fieldExists(entry, field):
    fieldName = prefix + field
    metric.fields[fieldName] = float(entry[field])


def addFieldString(entry, prefix, field, metric):
  if fieldExists(entry, field):
    fieldName = prefix + field
    metric.fields[fieldName] = str(entry[field])


def addFieldDate(entry, prefix, field, metric):
  if fieldExists(entry, field):
    fieldName = prefix + field
    metric.fields[fieldName] = time.now().unix_nano
    # metric.fields[fieldName] = time.parse_time(entry[field], format="1/2/2006 15:04:05")


def addFieldBool(entry, prefix, field, metric):
  if fieldExists(entry, field):
    fieldName = prefix + field
    metric.fields[fieldName] = bool(entry[field])


def addTime(entry, field, metric):
  metric.time = time.now().unix_nano
  # if fieldExists(entry, field):
  #   metric.time = time.parse_time(entry[field], format="1/2/2006 15:04:05")
  # else:
  #   metric.time = time.now().unix_nano


def fieldExists(obj, field):
  for key in obj.keys():
    if key == field:
      if obj[field] != None:
        return True
  return False