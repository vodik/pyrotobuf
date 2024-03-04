use prost_reflect::prost::Message as _;
use prost_reflect::text_format::FormatOptions;
use prost_reflect::{ReflectMessage, Value};
use pyo3::exceptions::{PyAttributeError, PyIndexError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
// use pythonize::Pythonizer;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;

#[pyclass(frozen)]
struct DescriptorPool(prost_reflect::DescriptorPool);

#[pymethods]
impl DescriptorPool {
    #[new]
    fn new(py: Python, bytes: &[u8]) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self(prost_reflect::DescriptorPool::decode(bytes.as_ref()).unwrap()),
        )
    }

    fn get_service_by_name(
        &self,
        py: Python,
        name: &str,
    ) -> PyResult<Option<Py<ServiceDescriptor>>> {
        let descriptor = self.0.get_service_by_name(name);
        descriptor
            .map(|descriptor| Py::new(py, ServiceDescriptor(descriptor)))
            .transpose()
    }

    fn get_message_by_name(
        &self,
        py: Python,
        name: &str,
    ) -> PyResult<Option<Py<MessageDescriptor>>> {
        let descriptor = self.0.get_message_by_name(name);
        descriptor
            .map(|descriptor| Py::new(py, MessageDescriptor(descriptor)))
            .transpose()
    }
}

#[pyclass(frozen)]
struct ServiceDescriptor(prost_reflect::ServiceDescriptor);

#[pymethods]
impl ServiceDescriptor {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[getter]
    fn full_name(&self) -> &str {
        self.0.full_name()
    }

    fn methods(&self, py: Python) -> PyResult<HashMap<String, Py<MethodDescriptor>>> {
        self.0
            .methods()
            .map(|method| Ok((method.name().into(), Py::new(py, MethodDescriptor(method))?)))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("<ServiceDescriptor('{}')>", self.0.full_name())
    }
}

#[pyclass(frozen)]
struct MethodDescriptor(prost_reflect::MethodDescriptor);

#[pymethods]
impl MethodDescriptor {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[getter]
    fn full_name(&self) -> &str {
        self.0.full_name()
    }

    #[getter]
    fn input_message(&self, py: Python) -> PyResult<Py<MessageDescriptor>> {
        Py::new(py, MessageDescriptor(self.0.input()))
    }

    #[getter]
    fn output_message(&self, py: Python) -> PyResult<Py<MessageDescriptor>> {
        Py::new(py, MessageDescriptor(self.0.output()))
    }
}

#[pyclass(frozen)]
struct MessageDescriptor(prost_reflect::MessageDescriptor);

#[pymethods]
impl MessageDescriptor {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[getter]
    fn full_name(&self) -> &str {
        self.0.full_name()
    }

    fn fields(&self, py: Python) -> PyResult<Vec<Py<FieldDescriptor>>> {
        self.0
            .fields()
            .map(|field| Py::new(py, FieldDescriptor(field)))
            .collect::<PyResult<_>>()
    }

    fn __repr__(&self) -> String {
        format!("<MessageDescriptor('{}')>", self.0.full_name())
    }
}

#[pyclass(frozen)]
struct FieldDescriptor(prost_reflect::FieldDescriptor);

#[pymethods]
impl FieldDescriptor {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[getter]
    fn full_name(&self) -> &str {
        self.0.full_name()
    }

    #[getter]
    fn number(&self) -> u32 {
        self.0.number()
    }

    #[getter]
    fn kind(&self, py: Python) -> PyObject {
        match self.0.kind() {
            prost_reflect::Kind::Double => "double".into_py(py),
            prost_reflect::Kind::Float => "float".into_py(py),
            prost_reflect::Kind::Int32 => "int32".into_py(py),
            prost_reflect::Kind::Int64 => "int64".into_py(py),
            prost_reflect::Kind::Uint32 => "uint32".into_py(py),
            prost_reflect::Kind::Uint64 => "uint64".into_py(py),
            prost_reflect::Kind::Sint32 => "sint32".into_py(py),
            prost_reflect::Kind::Sint64 => "sint64".into_py(py),
            prost_reflect::Kind::Fixed32 => "fixed32".into_py(py),
            prost_reflect::Kind::Fixed64 => "fixed64".into_py(py),
            prost_reflect::Kind::Sfixed32 => "sfixed32".into_py(py),
            prost_reflect::Kind::Sfixed64 => "sfixed64".into_py(py),
            prost_reflect::Kind::Bool => "bool".into_py(py),
            prost_reflect::Kind::String => "string".into_py(py),
            prost_reflect::Kind::Bytes => "bytes".into_py(py),
            prost_reflect::Kind::Message(message) => MessageDescriptor(message).into_py(py),
            prost_reflect::Kind::Enum(_) => todo!(),
        }
    }

    fn cadinality(&self, py: Python) -> PyObject {
        match self.0.cardinality() {
            prost_reflect::Cardinality::Optional => "optional".into_py(py),
            prost_reflect::Cardinality::Required => "required".into_py(py),
            prost_reflect::Cardinality::Repeated => "repeated".into_py(py),
        }
    }

    fn __repr__(&self) -> String {
        let kind: Cow<str> = match self.0.kind() {
            prost_reflect::Kind::Double => "double".into(),
            prost_reflect::Kind::Float => "float".into(),
            prost_reflect::Kind::Int32 => "int32".into(),
            prost_reflect::Kind::Int64 => "int64".into(),
            prost_reflect::Kind::Uint32 => "uint32".into(),
            prost_reflect::Kind::Uint64 => "uint64".into(),
            prost_reflect::Kind::Sint32 => "sint32".into(),
            prost_reflect::Kind::Sint64 => "sint64".into(),
            prost_reflect::Kind::Fixed32 => "fixed32".into(),
            prost_reflect::Kind::Fixed64 => "fixed64".into(),
            prost_reflect::Kind::Sfixed32 => "sfixed32".into(),
            prost_reflect::Kind::Sfixed64 => "sfixed64".into(),
            prost_reflect::Kind::Bool => "bool".into(),
            prost_reflect::Kind::String => "string".into(),
            prost_reflect::Kind::Bytes => "bytes".into(),
            prost_reflect::Kind::Message(message) => message.full_name().to_string().into(),
            prost_reflect::Kind::Enum(_) => todo!(),
        };

        let cardinality = match self.0.cardinality() {
            prost_reflect::Cardinality::Optional => "optional",
            prost_reflect::Cardinality::Required => "required",
            prost_reflect::Cardinality::Repeated => "repeated",
        };

        format!(
            "<FieldDescription(number={}, name='{}', kind=\'{}\', carnality=\'{}\')>",
            self.0.number(),
            self.0.name(),
            kind,
            cardinality,
        )
    }
}

#[pyclass(frozen)]
struct Field {
    #[pyo3(get)]
    descriptor: Py<FieldDescriptor>,
    #[pyo3(get)]
    value: PyObject,
}

#[pymethods]
impl Field {
    #[getter]
    fn name(&self) -> &str {
        self.descriptor.get().0.name()
    }

    #[getter]
    fn full_name(&self) -> &str {
        self.descriptor.get().0.full_name()
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        Ok(format!(
            "<Field(field='{}', value={}>",
            self.descriptor.get().__repr__(),
            self.value.as_ref(py).repr()?
        ))
    }
}

fn value_to_python(py: Python, value: &Value) -> PyObject {
    match value {
        Value::Bool(value) => value.into_py(py),
        Value::I32(value) => value.into_py(py),
        Value::I64(value) => value.into_py(py),
        Value::U32(value) => value.into_py(py),
        Value::U64(value) => value.into_py(py),
        Value::F32(value) => value.into_py(py),
        Value::F64(value) => value.into_py(py),
        Value::String(value) => value.into_py(py),
        Value::Bytes(value) => value.into_py(py),
        Value::EnumNumber(value) => value.into_py(py),
        Value::Message(value) => Message(value.clone()).into_py(py),
        Value::List(_) => todo!(),
        Value::Map(_) => todo!(),
    }
}

fn python_to_value(py: Python, value: &mut Value, obj: PyObject) -> PyResult<()> {
    match value {
        Value::Bool(value) => *value = obj.extract(py)?,
        Value::I32(value) => *value = obj.extract(py)?,
        Value::I64(value) => *value = obj.extract(py)?,
        Value::U32(value) => *value = obj.extract(py)?,
        Value::U64(value) => *value = obj.extract(py)?,
        Value::F32(value) => *value = obj.extract(py)?,
        Value::F64(value) => *value = obj.extract(py)?,
        Value::String(value) => *value = obj.extract(py)?,
        Value::Bytes(value) => *value = obj.extract::<Vec<u8>>(py)?.into(),
        Value::EnumNumber(value) => *value = obj.extract(py)?,
        Value::Message(value) => {
            let message = obj.extract::<Py<Message>>(py)?;
            *value = message.borrow(py).0.clone();
        }
        Value::List(_) => todo!(),
        Value::Map(_) => todo!(),
    }
    Ok(())
}

#[derive(FromPyObject)]
enum Format<'a> {
    Protobuf(&'a [u8]), // input is a positive int
    Text(&'a str),
}

#[pyclass]
#[derive(Clone)]
struct Message(prost_reflect::DynamicMessage);

#[pymethods]
impl Message {
    #[new]
    #[pyo3(signature = (descriptor, data = None, /, **kwargs))]
    fn new(
        py: Python,
        descriptor: &MessageDescriptor,
        data: Option<Format>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<Py<Self>> {
        let message = if let Some(format) = data {
            Self(match format {
                Format::Protobuf(bytes) => {
                    prost_reflect::DynamicMessage::decode(descriptor.0.clone(), bytes).unwrap()
                }
                Format::Text(text) => {
                    prost_reflect::DynamicMessage::parse_text_format(descriptor.0.clone(), text)
                        .unwrap()
                }
            })
        } else {
            let mut message = Self(prost_reflect::DynamicMessage::new(descriptor.0.clone()));

            if let Some(kwargs) = kwargs {
                for (key, value) in kwargs.iter() {
                    message.__setattr__(py, key.extract()?, value.into())?;
                }
            }

            message
        };

        Py::new(py, message)
    }

    #[getter]
    fn descriptor(&self, py: Python) -> PyResult<Py<MessageDescriptor>> {
        Py::new(py, MessageDescriptor(self.0.descriptor()))
    }

    fn fields(&self, py: Python) -> PyResult<Vec<Py<Field>>> {
        self.0
            .fields()
            .map(|(descriptor, value)| {
                let value = value_to_python(py, value);
                let descriptor = Py::new(py, FieldDescriptor(descriptor))?;
                Py::new(py, Field { value, descriptor })
            })
            .collect()
    }

    fn to_protobuf<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        PyBytes::new_with(py, self.0.encoded_len(), |bytes| {
            let output = self.0.encode_to_vec();
            bytes.copy_from_slice(&output[..]);
            Ok(())
        })
    }

    #[pyo3(signature = (*, pretty = false, skip_unknown_fields = true, expand_any = true))]
    fn to_text(&self, pretty: bool, skip_unknown_fields: bool, expand_any: bool) -> String {
        self.0.to_text_format_with_options(
            &FormatOptions::new()
                .pretty(pretty)
                .skip_unknown_fields(skip_unknown_fields)
                .expand_any(expand_any),
        )
    }

    fn to_json(&self) -> PyResult<Vec<u8>> {
        let mut serializer = serde_json::Serializer::new(vec![]);
        self.0.serialize(&mut serializer).unwrap();
        Ok(serializer.into_inner())
    }

    fn __eq__(&self, other: PyRef<Self>) -> bool {
        self.0 == other.0
    }

    fn __getattr__(&self, py: Python, name: &str) -> PyResult<PyObject> {
        self.0
            .get_field_by_name(name)
            .ok_or_else(|| PyAttributeError::new_err(name.to_string()))
            .map(|value| value_to_python(py, value.as_ref()))
    }

    fn __getitem__(&self, py: Python, number: u32) -> PyResult<PyObject> {
        self.0
            .get_field_by_number(number)
            .ok_or_else(|| PyIndexError::new_err(number.to_string()))
            .map(|value| value_to_python(py, value.as_ref()))
    }

    fn __setattr__(&mut self, py: Python, name: &str, obj: PyObject) -> PyResult<()> {
        self.0
            .get_field_by_name_mut(name)
            .ok_or_else(|| PyAttributeError::new_err(name.to_string()))
            .and_then(|value| python_to_value(py, value, obj))
    }

    fn __setitem__(&mut self, py: Python, number: u32, obj: PyObject) -> PyResult<()> {
        self.0
            .get_field_by_number_mut(number)
            .ok_or_else(|| PyIndexError::new_err(number.to_string()))
            .and_then(|value| python_to_value(py, value, obj))
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let fragments: Vec<String> = self
            .fields(py)?
            .into_iter()
            .map(|field| {
                let field = field.borrow(py);
                Ok(format!(
                    "{}={}",
                    field.descriptor.get().name(),
                    field.value.as_ref(py).repr()?,
                ))
            })
            .collect::<PyResult<_>>()?;

        let assignments = fragments.join(", ");
        Ok(format!(
            "Message('{}', {})",
            self.0.descriptor().full_name(),
            assignments
        ))
    }

    fn __bytes__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        self.to_protobuf(py)
    }

    fn __str__(&self) -> String {
        self.0.to_text_format()
    }
}

#[pymodule]
fn _pyrotobuf(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DescriptorPool>()?;
    m.add_class::<FieldDescriptor>()?;
    m.add_class::<MessageDescriptor>()?;
    m.add_class::<MethodDescriptor>()?;
    m.add_class::<ServiceDescriptor>()?;
    m.add_class::<Field>()?;
    m.add_class::<Message>()?;
    Ok(())
}
