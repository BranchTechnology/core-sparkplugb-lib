pub mod sparkplug_b;

use crate::sparkplug_b::{Payload, Payload_Metric, Payload_DataSet, Payload_Template};
use chrono;
use protobuf::RepeatedField;
use std::any::TypeId;
use std::mem::size_of;
use std::str::FromStr;
use serde::{Deserialize};


pub enum MessageType {
    NBIRTH = 0,
    // Birth certificate for MQTT EoN nodes.
    NDEATH = 1,
    // Death certificate for MQTT EoN nodes.
    DBIRTH = 2,
    // Birth certificate for Devices.
    DDEATH = 3,
    // Death certificate for Devices.
    NDATA = 4,
    // Node data message.
    DDATA = 5,
    // Device data message.
    NCMD = 6,
    // Node command message.
    DCMD = 7,
    // Device command message.
    STATE = 8,  // Critical application state message.
}

impl MessageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::NBIRTH => "NBIRTH",
            MessageType::NDEATH => "NDEATH",
            MessageType::DBIRTH => "DBIRTH",
            MessageType::DDEATH => "DDEATH",
            MessageType::NDATA => "NDATA",
            MessageType::DDATA => "DDATA",
            MessageType::NCMD => "NCMD",
            MessageType::DCMD => "DCMD",
            MessageType::STATE => "STATE",
        }
    }
}

// import sparkplug_b_pb2
// import time
// from sparkplug_b_pb2 import Payload
//
// seqNum = 0
// bdSeq = 0
//
// class DataSetDataType:
// Unknown = 0
// Int8 = 1
// Int16 = 2
// Int32 = 3
// Int64 = 4
// UInt8 = 5
// UInt16 = 6
// UInt32 = 7
// UInt64 = 8
// Float = 9
// Double = 10
// Boolean = 11
// String = 12
// DateTime = 13
// Text = 14
//
#[derive(Deserialize, Debug, PartialEq, Clone, Copy)]
pub enum MetricDataType {
    Unknown = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float = 9,
    Double = 10,
    Boolean = 11,
    String = 12,
    DateTime = 13,
    Text = 14,
    UUID = 15,
    DataSet = 16,
    Bytes = 17,
    File = 18,
    Template = 19,
}


impl FromStr for MetricDataType {
    type Err = ();

    fn from_str(input: &str) -> Result<MetricDataType, Self::Err> {
        match input {
            "Unknown" => Ok(MetricDataType::Unknown),
            "Int8" => Ok(MetricDataType::Int8),
            "Int16" => Ok(MetricDataType::Int16),
            "Int32" => Ok(MetricDataType::Int32),
            "Int64" => Ok(MetricDataType::Int64),
            "UInt8" => Ok(MetricDataType::UInt8),
            "UInt16" => Ok(MetricDataType::UInt16),
            "UInt32" => Ok(MetricDataType::UInt32),
            "UInt64" => Ok(MetricDataType::UInt64),
            "Float" => Ok(MetricDataType::Float),
            "Double" => Ok(MetricDataType::Double),
            "Boolean" => Ok(MetricDataType::Boolean),
            "String" => Ok(MetricDataType::String),
            "DateTime" => Ok(MetricDataType::DateTime),
            "Text" => Ok(MetricDataType::Text),
            "UUID" => Ok(MetricDataType::UUID),
            "DataSet" => Ok(MetricDataType::DataSet),
            "Bytes" => Ok(MetricDataType::Bytes),
            "File" => Ok(MetricDataType::File),
            "Template" => Ok(MetricDataType::Template),
            _ => Err(()),
        }
    }
}

// ######################################################################
// # Always request this before requesting the Node Birth Payload
// ######################################################################
// def getNodeDeathPayload():
// payload = sparkplug_b_pb2.Payload()
// addMetric(payload, "bdSeq", None, MetricDataType.Int64, get_bd_seq_num())
// return payload
// ######################################################################
// Not sure if this conforms with how most mqtt libs deal with will message...
// They serialize the message before hand, so `get_bd_seq_num` will always be 0...
// TODO (lower priority) explore if there's a way to call this lazily if not the expectation is this cant be reliant will message
pub fn get_node_death_payload() -> Payload {
    let mut pm = Payload::new();
    let mut container: Vec<Payload_Metric> = vec![];
    let bseq = unsafe { get_bd_seq_num() };
    println!("{:?}", bseq);
    let metric = create_metric(MetricDataType::Int64, bseq, "bdSeq".into(), None, None);
    container.push(metric);
    pm.set_metrics(RepeatedField::from_vec(container));
    pm
}

// ######################################################################
// # Always request this after requesting the Node Death Payload
// ######################################################################
// def getNodeBirthPayload():
// global seqNum
// seqNum = 0
// payload = sparkplug_b_pb2.Payload()
// payload.timestamp = int(round(time.time() * 1000))
// payload.seq = getSeqNum()
// addMetric(payload, "bdSeq", None, MetricDataType.Int64, --bdSeq)
// return payload
fn get_node_birth_payload() -> Payload {
    let mut pm = Payload::new();
    pm.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);
    let mut container: Vec<Payload_Metric> = vec![];
    let bseq = unsafe { get_bd_seq_num() };
    let metric = create_metric(MetricDataType::Int64, bseq, "bdSeq".into(), None, None);
    container.push(metric);
    pm.set_metrics(RepeatedField::from_vec(container));
    pm
}

// ######################################################################
//
// ######################################################################
// # Get the DBIRTH payload
// ######################################################################
// def getDeviceBirthPayload():
// payload = sparkplug_b_pb2.Payload()
// payload.timestamp = int(round(time.time() * 1000))
// payload.seq = getSeqNum()
// return payload
pub fn get_device_birth_payload() -> Payload {
    let mut pm = Payload::new();
    pm.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);
    let seq = unsafe { get_seq_num() };
    pm.set_seq(seq);
    pm
}

// ######################################################################
//
// ######################################################################
// # Get a DDATA payload
// ######################################################################
// def getDdataPayload():
// return getDeviceBirthPayload()
fn get_d_data_payload() -> Payload {
    get_device_birth_payload()
}
// ######################################################################
//
// ######################################################################
// # Helper method for adding dataset metrics to a payload
// ######################################################################
// def initDatasetMetric(payload, name, alias, columns, types):
// metric = payload.metrics.add()
// if name is not None:
// metric.name = name
// if alias is not None:
// metric.alias = alias
// metric.timestamp = int(round(time.time() * 1000))
// metric.datatype = MetricDataType.DataSet
//
// # Set up the dataset
// metric.dataset_value.num_of_columns = len(types)
// metric.dataset_value.columns.extend(columns)
// metric.dataset_value.types.extend(types)
// return metric.dataset_value

fn init_dataset_metric(
    name: String,
    alias: Option<u64>,
    columns: Vec<String>,
    rows_types: Vec<u32>,
) -> Payload_Metric {
    let mut metric = Payload_Metric::new();
    metric.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);

    metric.set_name(name);
    match alias {
        Some(p) => metric.set_alias(p),
        _ => {}
    }
    metric.set_datatype(MetricDataType::DataSet as u32);

    let mut dataset = Payload_DataSet::new();
    dataset.set_num_of_columns(columns.len() as u64);
    dataset.set_columns(RepeatedField::from(columns));
    dataset.set_types(rows_types);

    metric.set_dataset_value(dataset);
    metric
}

// ######################################################################
//
// ######################################################################
// # Helper method for adding dataset metrics to a payload
// ######################################################################
// def initTemplateMetric(payload, name, alias, templateRef):
// metric = payload.metrics.add()
// if name is not None:
// metric.name = name
// if alias is not None:
// metric.alias = alias
// metric.timestamp = int(round(time.time() * 1000))
// metric.datatype = MetricDataType.Template
//
// # Set up the template
// if templateRef is not None:
// metric.template_value.template_ref = templateRef
// metric.template_value.is_definition = False
// else:
// metric.template_value.is_definition = True
//
// return metric.template_value
fn init_template_metric(
    mut containter: Vec<Payload_Metric>,
    name: String,
    alias: Option<u64>,
    template_ref: Payload_Template,
) {
    let mut metric = Payload_Metric::new();
    metric.set_name(name);
    metric.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);

    match alias {
        Some(p) => metric.set_alias(p),
        _ => {}
    }
    metric.set_template_value(template_ref);
    containter.push(metric);
}

// ######################################################################
//
// ######################################################################
// # Helper method for adding metrics to a container which can be a
// # payload or a template
// ######################################################################
// def addMetric(container, name, alias, type, value):
// metric = container.metrics.add()
// if name is not None:
// metric.name = name
// if alias is not None:
// metric.alias = alias
// metric.timestamp = int(round(time.time() * 1000))
//

// elif data_type == MetricDataType.Int16:
// metric.datatype = MetricDataType.Int16
// metric.int_value = value
// elif data_type == MetricDataType.Int32:
// metric.datatype = MetricDataType.Int32
// metric.int_value = value
// elif data_type == MetricDataType.Int64:
// metric.datatype = MetricDataType.Int64
// metric.long_value = value
// elif data_type == MetricDataType.UInt8:
// metric.datatype = MetricDataType.UInt8
// metric.int_value = value
// elif data_type == MetricDataType.UInt16:
// metric.datatype = MetricDataType.UInt16
// metric.int_value = value
// elif data_type == MetricDataType.UInt32:
// metric.datatype = MetricDataType.UInt32
// metric.int_value = value
// elif data_type == MetricDataType.UInt64:
// metric.datatype = MetricDataType.UInt64
// metric.long_value = value
// elif data_type == MetricDataType.Float:
// metric.datatype = MetricDataType.Float
// metric.float_value = value
// elif data_type == MetricDataType.Double:
// metric.datatype = MetricDataType.Double
// metric.double_value = value
// elif data_type == MetricDataType.Boolean:
// metric.datatype = MetricDataType.Boolean
// metric.boolean_value = value
// elif data_type == MetricDataType.String:
// metric.datatype = MetricDataType.String
// metric.string_value = value
// elif data_type == MetricDataType.DateTime:
// metric.datatype = MetricDataType.DateTime
// metric.long_value = value
// elif data_type == MetricDataType.Text:
// metric.datatype = MetricDataType.Text
// metric.string_value = value
// elif data_type == MetricDataType.UUID:
// metric.datatype = MetricDataType.UUID
// metric.string_value = value
// elif data_type == MetricDataType.Bytes:
// metric.datatype = MetricDataType.Bytes
// metric.bytes_value = value
// elif data_type == MetricDataType.File:
// metric.datatype = MetricDataType.File
// metric.bytes_value = value
// elif data_type == MetricDataType.Template:
// metric.datatype = MetricDataType.Template
// metric.template_value = value
// else:
// print "Invalid: " + str(data_type)
// }
// # Return the metric
// return metric
// ######################################################################
pub fn create_metric<T: 'static>(
    data_type: MetricDataType,
    value: T,
    name: String,
    alias: Option<u64>,
    historical: Option<bool>,
) -> Payload_Metric {
    let mut metric = Payload_Metric::new();
    metric.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);
    match historical {
        Some(p) => metric.set_is_historical(p),
        _ => {}
    }
    metric.set_name(name);

    match alias {
        Some(p) => metric.set_alias(p),
        _ => {}
    }


    set_metric_type(data_type, &mut metric);
    set_metric_value(data_type, &mut metric, value);
    metric
}

pub fn equals<U: 'static, V: 'static>() -> bool {
    TypeId::of::<U>() == TypeId::of::<V>() && size_of::<U>() == size_of::<V>()
}

// TODO no idea I guess this works...
// https://github.com/keyvank/generic-cast
pub fn cast_ref<U: 'static, V: 'static>(u: &U) -> Option<&V> {
    if equals::<U, V>() {
        Some(unsafe { std::mem::transmute::<&U, &V>(u) })
    } else {
        None
    }
}

pub fn cast_mut<U: 'static, V: 'static>(u: &mut U) -> Option<&mut V> {
    if equals::<U, V>() {
        Some(unsafe { std::mem::transmute::<&mut U, &mut V>(u) })
    } else {
        None
    }
}

pub fn create_metric_from_str(
    data_type: MetricDataType,
    value: &str,
    name: String,
    alias: Option<u64>,
    historical: Option<bool>,
) -> Payload_Metric {
    let mut metric = Payload_Metric::new();
    metric.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);
    match historical {
        Some(p) => metric.set_is_historical(p),
        _ => {}
    }
    metric.set_name(name);

    match alias {
        Some(p) => metric.set_alias(p),
        _ => {}
    }
    set_metric_type(data_type, &mut metric);
    set_str_metric_value(data_type, &mut metric, value);
    metric
}

fn set_str_metric_value(data_type: MetricDataType, metric: &mut Payload_Metric, str: &str) {
    match data_type {
        MetricDataType::Int8 => metric.set_int_value(str.parse().unwrap()),
        MetricDataType::Int16 => metric.set_int_value(str.parse().unwrap()),
        MetricDataType::Int32 => metric.set_int_value(str.parse().unwrap()),
        MetricDataType::Int64 => metric.set_long_value(str.parse().unwrap()),
        MetricDataType::UInt8 => metric.set_int_value(str.parse().unwrap()),
        MetricDataType::UInt16 => metric.set_int_value(str.parse().unwrap()),
        MetricDataType::UInt32 => metric.set_int_value(str.parse().unwrap()),
        MetricDataType::UInt64 => metric.set_long_value(str.parse().unwrap()),
        MetricDataType::Float => metric.set_float_value(str.parse().unwrap()),
        MetricDataType::Double => metric.set_double_value(str.parse().unwrap()),
        MetricDataType::Boolean => metric.set_boolean_value(str.parse().unwrap()),
        MetricDataType::String => metric.set_string_value(str.parse().unwrap()),
        MetricDataType::DateTime => metric.set_long_value(str.parse().unwrap()),
        MetricDataType::Text => metric.set_string_value(str.parse().unwrap()),
        MetricDataType::UUID => metric.set_string_value(str.parse().unwrap()),
        // MetricDataType::Bytes => metric.set_bytes_value(str.parse().unwrap()),
        // MetricDataType::File => metric.set_bytes_value(str.parse().unwrap()),
        _ => (),
    }
}

fn set_metric_value<T: 'static>(data_type: MetricDataType, metric: &mut Payload_Metric, value: T) {
    match data_type {
        MetricDataType::Int8 => metric.set_int_value(*cast_ref::<T, u32>(&value).unwrap()),
        MetricDataType::Int16 => metric.set_int_value(*cast_ref::<T, u32>(&value).unwrap()),
        MetricDataType::Int32 => metric.set_int_value(*cast_ref::<T, u32>(&value).unwrap()),
        MetricDataType::Int64 => metric.set_long_value(*cast_ref::<T, u64>(&value).unwrap()),
        MetricDataType::UInt8 => metric.set_int_value(*cast_ref::<T, u32>(&value).unwrap()),
        MetricDataType::UInt16 => metric.set_int_value(*cast_ref::<T, u32>(&value).unwrap()),
        MetricDataType::UInt32 => metric.set_int_value(*cast_ref::<T, u32>(&value).unwrap()),
        MetricDataType::UInt64 => metric.set_long_value(*cast_ref::<T, u64>(&value).unwrap()),
        MetricDataType::Float => metric.set_float_value(*cast_ref::<T, f32>(&value).unwrap()),
        MetricDataType::Double => metric.set_double_value(*cast_ref::<T, f64>(&value).unwrap()),
        MetricDataType::Boolean => metric.set_boolean_value(*cast_ref::<T, bool>(&value).unwrap()),
        MetricDataType::String => metric.set_string_value(cast_ref::<T, String>(&value).unwrap().clone()),
        MetricDataType::DateTime => metric.set_long_value(*cast_ref::<T, u64>(&value).unwrap()),
        MetricDataType::Text => metric.set_string_value(cast_ref::<T, String>(&value).unwrap().clone()),
        MetricDataType::UUID => metric.set_string_value(cast_ref::<T, String>(&value).unwrap().clone()),
        MetricDataType::Bytes => metric.set_bytes_value(cast_ref::<T, Vec<u8>>(&value).unwrap().clone()),
        MetricDataType::File => metric.set_bytes_value(cast_ref::<T, Vec<u8>>(&value).unwrap().clone()),
        MetricDataType::Template => metric.set_template_value(cast_ref::<T, Payload_Template>(&value).unwrap().clone()),
        _ => (),
    }
}

fn set_metric_type(data_type: MetricDataType, metric: &mut Payload_Metric) {
    match data_type {
        MetricDataType::Int8 => metric.set_datatype(data_type as u32),
        MetricDataType::Int16 => metric.set_datatype(data_type as u32),
        MetricDataType::Int32 => metric.set_datatype(data_type as u32),
        MetricDataType::Int64 => metric.set_datatype(data_type as u32),
        MetricDataType::UInt8 => metric.set_datatype(data_type as u32),
        MetricDataType::UInt16 => metric.set_datatype(data_type as u32),
        MetricDataType::UInt32 => metric.set_datatype(data_type as u32),
        MetricDataType::UInt64 => metric.set_datatype(data_type as u32),
        MetricDataType::Float => metric.set_datatype(data_type as u32),
        MetricDataType::Double => metric.set_datatype(data_type as u32),
        MetricDataType::Boolean => metric.set_datatype(data_type as u32),
        MetricDataType::String => metric.set_datatype(data_type as u32),
        MetricDataType::DateTime => metric.set_datatype(data_type as u32),
        MetricDataType::Text => metric.set_datatype(data_type as u32),
        MetricDataType::UUID => metric.set_datatype(data_type as u32),
        MetricDataType::Bytes => metric.set_datatype(data_type as u32),
        MetricDataType::File => metric.set_datatype(data_type as u32),
        MetricDataType::Template => metric.set_datatype(data_type as u32),
        _ => (),
    }
}

// ######################################################################
// # Helper method for adding metrics to a container which can be a
// # payload or a template
// ######################################################################
// def addNullMetric(container, name, alias, type):
// metric = container.metrics.add()
// if name is not None:
// metric.name = name
// if alias is not None:
// metric.alias = alias
// metric.timestamp = int(round(time.time() * 1000))
// metric.is_null = True
//
// # print "Type: " + str(type)
//
// if type == MetricDataType.Int8:
// metric.datatype = MetricDataType.Int8
// elif type == MetricDataType.Int16:
// metric.datatype = MetricDataType.Int16
// elif type == MetricDataType.Int32:
// metric.datatype = MetricDataType.Int32
// elif type == MetricDataType.Int64:
// metric.datatype = MetricDataType.Int64
// elif type == MetricDataType.UInt8:
// metric.datatype = MetricDataType.UInt8
// elif type == MetricDataType.UInt16:
// metric.datatype = MetricDataType.UInt16
// elif type == MetricDataType.UInt32:
// metric.datatype = MetricDataType.UInt32
// elif type == MetricDataType.UInt64:
// metric.datatype = MetricDataType.UInt64
// elif type == MetricDataType.Float:
// metric.datatype = MetricDataType.Float
// elif type == MetricDataType.Double:
// metric.datatype = MetricDataType.Double
// elif type == MetricDataType.Boolean:
// metric.datatype = MetricDataType.Boolean
// elif type == MetricDataType.String:
// metric.datatype = MetricDataType.String
// elif type == MetricDataType.DateTime:
// metric.datatype = MetricDataType.DateTime
// elif type == MetricDataType.Text:
// metric.datatype = MetricDataType.Text
// elif type == MetricDataType.UUID:
// metric.datatype = MetricDataType.UUID
// elif type == MetricDataType.Bytes:
// metric.datatype = MetricDataType.Bytes
// elif type == MetricDataType.File:
// metric.datatype = MetricDataType.File
// elif type == MetricDataType.Template:
// metric.datatype = MetricDataType.Template
// else:
// print "Invalid: " + str(type)
//
// # Return the metric
// return metric
// ######################################################################
//
fn add_null_metric(
    data_type: MetricDataType,
    name: String,
    alias: Option<u64>,
    historical: Option<bool>,
) -> Payload_Metric {
    let mut metric = Payload_Metric::new();
    metric.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);

    match historical {
        Some(p) => metric.set_is_historical(p),
        _ => {}
    }
    metric.set_name(name);

    match alias {
        Some(p) => metric.set_alias(p),
        _ => {}
    }

    set_metric_type(data_type, &mut metric);
    metric
}

// ######################################################################
// # Helper method for getting the next sequence number
// ######################################################################
// def getSeqNum():
// global seqNum
// retVal = seqNum
// # print("seqNum: " + str(retVal))
// seqNum += 1
// if seqNum == 256:
// seqNum = 0
// return retVal
// ######################################################################
// TODO revisit if we can remove global
static mut SEQ: u64 = 0;

unsafe fn get_seq_num() -> u64 {
    let ret_val = SEQ;
    SEQ += 1;
    if SEQ == 256 {
        SEQ = 0;
    }
    ret_val
}

// ######################################################################
// # Helper method for getting the next birth/death sequence number
// ######################################################################
// def get_bd_seq_num():
// global bdSeq
// retVal = bdSeq
// # print("bdSeqNum: " + str(retVal))
// bdSeq += 1
// if bdSeq == 256:
// bdSeq = 0
// return retVal
// ######################################################################

// TODO revisit if we can remove global
static mut BDSEQ: u64 = 0;

unsafe fn get_bd_seq_num() -> u64 {
    let ret_val = BDSEQ;
    BDSEQ += 1;
    if BDSEQ == 256 {
        BDSEQ = 0;
    }
    ret_val
}
