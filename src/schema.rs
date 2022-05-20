use bytes::{BufMut, BytesMut};
use log::{debug, error};
use protobuf::{Message, RepeatedField};
use serde::de::Unexpected::Str;
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use spark_rust::sparkplug_b::{
    DataType, Payload, Payload_MetaData, Payload_Metric, Payload_PropertySet, Payload_PropertyValue,
};
use spark_rust::{ MetricDataType, PropertyDataType};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::{fmt, io};
use std::str::{FromStr, ParseBoolError};
use serde::de::Error;
use crate::sparkplug_error::SparkplugError; // (1)


#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct PropertyValue {
    pub is_null: Option<bool>,
    pub value: String,
    pub data_type: PropertyDataType,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct PropertySet {
    pub keys: Vec<String>,
    pub values: Vec<PropertyValue>
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Metadata {
    pub is_multi_part: Option<bool>,
    // General metadata
    pub content_type: Option<String>,
    pub size: Option<u64>,
    pub seq: Option<u64>,

    // File metadata
    pub file_name: Option<String>,
    pub file_type: Option<String>,
    pub md5: Option<String>,

    // Catchalls and future expansion
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum MetricValue {
    IntValue(u32),
    LongValue(u64),
    FloatValue(f32),
    DoubleValue(f64),
    BooleanValue(bool),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Metric {
    pub name: Option<String>,
    pub value: Option<MetricValue>,
    pub data_type: Option<MetricDataType>,
    pub alias: Option<u64>,
    pub historical: Option<bool>,
    pub property_set: Option<PropertySet>,
    pub metadata: Option<Metadata>,
    pub is_transient: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct SparkplugB {
    pub metrics: Vec<Metric>,
    pub timestamp: Option<u64>,
    pub seq: Option<u64>,
    pub uuid: Option<String>,
    pub body: Option<String>,
}


#[cfg(test)]
mod sparkplug_test {
    use crate::sparkplug::sparkplugb::{Metric, MetricValue, PropertySet, SparkplugB};

    #[test]
    fn construction (){
        let l = SparkplugB{
            metrics: vec![Metric{
                name: None,
                value: Some(MetricValue::IntValue(12)),
                data_type: None,
                alias: None,
                historical: None,
                property_set: Some(PropertySet{ keys: vec![], values: vec![] }),
                metadata: None,
                is_transient: None
            }],
            timestamp: None,
            seq: None,
            uuid: None,
            body: None
        };


    }
}