use log::{error, info};
use spark_rust::sparkplug_b::{Payload, Payload_MetaData, Payload_Metric, Payload_PropertySet, Payload_PropertyValue};
use spark_rust::{MetricDataType, PropertyDataType};
use protobuf::RepeatedField;
use crate::schema;
use crate::schema::{SparkplugB, Metric, PropertyValue, PropertySet, Metadata, Metrics};
use crate::sparkplug_error::SparkplugError;

impl Proto<Payload_PropertyValue> for PropertyValue {
    fn form_proto(&self) -> Result<Payload_PropertyValue, SparkplugError> {
        let mut prop = Payload_PropertyValue::new();

        prop = match self.parse_value(prop) {
            Ok(p) => p,
            Err(e) => {
                return Err(e);
            }
        };

        prop.set_field_type(self.data_type as u32);

        match self.is_null {
            Some(b) => prop.set_is_null(b),
            _ => {}
        }
        Ok(prop)
    }
}

impl ProtoValue<Payload_PropertyValue> for PropertyValue {
    fn parse_value(&self, mut prop: Payload_PropertyValue) -> Result<Payload_PropertyValue, SparkplugError> {
        match self.data_type {
            PropertyDataType::Unknown => prop.set_string_value(self.value.clone()),
            PropertyDataType::Int8 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Int16 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Int32 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Int64 => {
                match self.value.parse() {
                    Ok(t) => prop.set_long_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u64: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::UInt8 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::UInt16 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::UInt32 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::UInt64 => {
                match self.value.parse() {
                    Ok(t) => prop.set_long_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u64: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Float => {
                match self.value.parse() {
                    Ok(t) => prop.set_float_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to f32: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Double => {
                match self.value.parse() {
                    Ok(t) => prop.set_double_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to f64: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Boolean => {
                match schema::parse_boolean(self.value.as_str()) {
                    Ok(b) => prop.set_boolean_value(b),
                    Err(e) => {
                        return Err(SparkplugError::new("failed to cast string to boolean"));
                    }
                }
            }
            PropertyDataType::String => prop.set_string_value(self.value.clone()),
            PropertyDataType::DateTime => {
                match self.value.parse() {
                    Ok(t) => prop.set_long_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u64: {:?}", e).as_str()));
                    }
                }
            }
            PropertyDataType::Text => prop.set_string_value(self.value.clone()),
        };
        Ok(prop)
    }
}


impl Proto<Payload_PropertySet> for PropertySet {
    fn form_proto(&self) -> Result<Payload_PropertySet, SparkplugError> {
        let mut property_set = Payload_PropertySet::new();
        let mut property_keys: Vec<String> = vec![];
        let mut property_values: Vec<Payload_PropertyValue> = vec![];
        for (key, prop_val) in &self.value {
            match prop_val.form_proto() {
                Ok(v) => {
                    property_keys.push(key.clone());
                    property_values.push(v)
                }
                Err(e) => error!("failed to parse property value: {:?}", e) //just skip the property
            }
        }
        property_set.set_keys(RepeatedField::from_vec(property_keys));
        property_set.set_values(RepeatedField::from_vec(property_values));
        Ok(property_set)
    }
}


pub trait Proto<T> {
    fn form_proto(&self) -> Result<T, SparkplugError>;
}

pub trait ProtoValue<T> {
    fn parse_value(&self, prop: T) -> Result<T, SparkplugError>;
}

impl Proto<Payload_MetaData> for Metadata {
    fn form_proto(&self) -> Result<Payload_MetaData, SparkplugError> {
        let mut md = Payload_MetaData::new();
        if let Some(v) = &self.is_multi_part {
            md.set_is_multi_part(*v);
        }
        if let Some(v) = &self.content_type {
            md.set_content_type(v.clone());
        }
        if let Some(v) = &self.size {
            md.set_size(*v);
        }
        if let Some(v) = &self.seq {
            md.set_seq(*v);
        }
        if let Some(v) = &self.file_name {
            md.set_file_name(v.clone());
        }
        if let Some(v) = &self.file_type {
            md.set_file_type(v.clone());
        }
        if let Some(v) = &self.md5 {
            md.set_md5(v.clone());
        }
        if let Some(v) = &self.description {
            md.set_description(v.clone());
        }
        Ok(md)
    }
}

trait ProtoMetric {
    fn form_proto(&self, name: String) -> Result<Payload_Metric, SparkplugError>;
}

impl ProtoMetric for Metric {
    fn form_proto(&self, name: String) -> Result<Payload_Metric, SparkplugError> {
        let mut metric = Payload_Metric::new();
        metric = match self.parse_value(metric) {
            Ok(p) => p,
            Err(e) => {
                return Err(e);
            }
        };
        metric.set_timestamp(chrono::offset::Utc::now().timestamp() as u64);
        metric.set_datatype(self.data_type as u32);

        if let Some(alias) = &self.alias {
            metric.set_alias(*alias);
        }

        if let Some(historical) = &self.historical {
            metric.set_is_historical(*historical);
        }

        if let Some(is_transient) = &self.is_transient {
            metric.set_is_transient(*is_transient);
        }

        if let Some(v) = &self.metadata {
            match v.form_proto() {
                Ok(metadata_payload) => metric.set_metadata(metadata_payload),
                Err(e) => error!("failed to parse metadata from the struct to protobuf: {:?}", e)
            }
        }
        if let Some(prop_set) = &self.property_set {
            match prop_set.form_proto() {
                Ok(p) => metric.set_properties(p),
                Err(e) => {
                    error!("failed to write property set");
                }
            }
        }
        Ok(metric)
    }
}

impl ProtoValue<Payload_Metric> for Metric {
    fn parse_value(&self, mut prop: Payload_Metric) -> Result<Payload_Metric, SparkplugError> {
        match self.data_type {
            MetricDataType::Unknown => prop.set_string_value(self.value.clone()),
            MetricDataType::Int8 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Int16 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Int32 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Int64 => {
                match self.value.parse() {
                    Ok(t) => prop.set_long_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u64: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::UInt8 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::UInt16 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::UInt32 => {
                match self.value.parse() {
                    Ok(t) => prop.set_int_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::UInt64 => {
                match self.value.parse() {
                    Ok(t) => prop.set_long_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u64: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Float => {
                match self.value.parse() {
                    Ok(t) => prop.set_float_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to f32: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Double => {
                match self.value.parse() {
                    Ok(t) => prop.set_double_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to f64: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Boolean => {
                match schema::parse_boolean(self.value.as_str()) {
                    Ok(b) => prop.set_boolean_value(b),
                    Err(e) => {
                        return Err(SparkplugError::new("failed to cast string to boolean"));
                    }
                }
            }
            MetricDataType::String => prop.set_string_value(self.value.clone()),
            MetricDataType::DateTime => {
                match self.value.parse() {
                    Ok(t) => prop.set_long_value(t),
                    Err(e) => {
                        return Err(SparkplugError::new(format!("failed to cast string to u64: {:?}", e).as_str()));
                    }
                }
            }
            MetricDataType::Text => prop.set_string_value(self.value.clone()),
            _ => {
                return Err(SparkplugError::new(format!("failed to cast to type, not implemented").as_str()));
            }
        };
        Ok(prop)
    }
}


impl Proto<Vec<Payload_Metric>> for Metrics {
    fn form_proto(&self) -> Result<Vec<Payload_Metric>, SparkplugError> {
        let mut metrics: Vec<Payload_Metric> = vec![];
        for (key, val) in &self.value {
            match val.form_proto(key.clone()) {
                Ok(metric) => metrics.push(metric),
                Err(e) => error!("failed to add metric: {} reason: {:?}", key, val) // ignore errors for individual metrics
            }
        }
        Ok(metrics)
    }
}

impl Proto<Payload> for SparkplugB {
    fn form_proto(&self) -> Result<Payload, SparkplugError> {
        let mut payload = Payload::new();
        payload.set_uuid(self.name.clone());
        match self.timestamp{
            Some(time)=> payload.set_timestamp(time),
            _=> payload.set_timestamp(chrono::offset::Utc::now().timestamp() as u64)
        }
        match self.seq{
            Some(seq)=> payload.set_seq(seq),
            _=> info!("Setting the sequence in SparkplugB is highly recommended")
        }
        match self.metrics.form_proto() {
            Ok(m) => payload.set_metrics(RepeatedField::from_vec(m)),
            Err(e) => error!("failed to create metrics: {:?}", e)
        }
        Ok(payload)
    }
}

pub trait Name {
    fn get_name(&self) -> String;
}

impl Name for SparkplugB {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}
