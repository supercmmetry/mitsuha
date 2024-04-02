use std::str::FromStr;

use anyhow::anyhow;
use chrono::{DateTime, NaiveDateTime, Utc};
use mitsuha_core_types::{
    channel::{ComputeInput, ComputeOutput},
    kernel::{JobSpec, JobStatus, JobStatusType, StorageSpec},
    module::{ModuleInfo, ModuleType},
    symbol::Symbol,
};

use crate::proto;

pub mod channel_proto {
    include!("../proto/channel.rs");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("channel_descriptor");
}

impl TryInto<ComputeInput> for proto::channel::ComputeRequest {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ComputeInput, Self::Error> {
        match self
            .compute_request_one_of
            .ok_or(anyhow!("could not find compute_request_one_of"))?
        {
            proto::channel::compute_request::ComputeRequestOneOf::Store(x) => {
                Ok(ComputeInput::Store {
                    spec: x.spec.ok_or(anyhow!("cannot find spec"))?.try_into()?,
                })
            }
            proto::channel::compute_request::ComputeRequestOneOf::Load(x) => {
                Ok(ComputeInput::Load {
                    handle: x.handle,
                    extensions: x.extensions,
                })
            }
            proto::channel::compute_request::ComputeRequestOneOf::Persist(x) => {
                Ok(ComputeInput::Persist {
                    handle: x.handle,
                    ttl: x.ttl,
                    extensions: x.extensions,
                })
            }
            proto::channel::compute_request::ComputeRequestOneOf::Clear(x) => {
                Ok(ComputeInput::Clear {
                    handle: x.handle,
                    extensions: x.extensions,
                })
            }
            proto::channel::compute_request::ComputeRequestOneOf::Run(x) => Ok(ComputeInput::Run {
                spec: x.spec.ok_or(anyhow!("cannot find spec"))?.try_into()?,
            }),
            proto::channel::compute_request::ComputeRequestOneOf::Extend(x) => {
                Ok(ComputeInput::Extend {
                    handle: x.handle,
                    ttl: x.ttl,
                    extensions: x.extensions,
                })
            }
            proto::channel::compute_request::ComputeRequestOneOf::Status(x) => {
                Ok(ComputeInput::Status {
                    handle: x.handle,
                    extensions: x.extensions,
                })
            }
            proto::channel::compute_request::ComputeRequestOneOf::Abort(x) => {
                Ok(ComputeInput::Abort {
                    handle: x.handle,
                    extensions: x.extensions,
                })
            }
        }
    }
}

impl TryFrom<ComputeInput> for proto::channel::ComputeRequest {
    type Error = anyhow::Error;

    fn try_from(value: ComputeInput) -> Result<Self, Self::Error> {
        let one_of: anyhow::Result<proto::channel::compute_request::ComputeRequestOneOf> =
            match value {
                ComputeInput::Store { spec } => {
                    Ok(proto::channel::compute_request::ComputeRequestOneOf::Store(
                        proto::channel::StoreRequest {
                            spec: Some(spec.try_into()?),
                        },
                    ))
                }
                ComputeInput::Load { handle, extensions } => {
                    Ok(proto::channel::compute_request::ComputeRequestOneOf::Load(
                        proto::channel::LoadRequest { handle, extensions },
                    ))
                }
                ComputeInput::Persist {
                    handle,
                    ttl,
                    extensions,
                } => Ok(
                    proto::channel::compute_request::ComputeRequestOneOf::Persist(
                        proto::channel::PersistRequest {
                            handle,
                            ttl,
                            extensions,
                        },
                    ),
                ),
                ComputeInput::Clear { handle, extensions } => {
                    Ok(proto::channel::compute_request::ComputeRequestOneOf::Clear(
                        proto::channel::ClearRequest { handle, extensions },
                    ))
                }
                ComputeInput::Run { spec } => {
                    Ok(proto::channel::compute_request::ComputeRequestOneOf::Run(
                        proto::channel::RunRequest {
                            spec: Some(spec.try_into()?),
                        },
                    ))
                }
                ComputeInput::Extend {
                    handle,
                    ttl,
                    extensions,
                } => Ok(
                    proto::channel::compute_request::ComputeRequestOneOf::Extend(
                        proto::channel::ExtendRequest {
                            handle,
                            ttl,
                            extensions,
                        },
                    ),
                ),
                ComputeInput::Status { handle, extensions } => Ok(
                    proto::channel::compute_request::ComputeRequestOneOf::Status(
                        proto::channel::StatusRequest { handle, extensions },
                    ),
                ),
                ComputeInput::Abort { handle, extensions } => {
                    Ok(proto::channel::compute_request::ComputeRequestOneOf::Abort(
                        proto::channel::AbortRequest { handle, extensions },
                    ))
                }
            };

        Ok(proto::channel::ComputeRequest {
            compute_request_one_of: Some(one_of?),
        })
    }
}

impl TryInto<ComputeOutput> for proto::channel::ComputeResponse {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ComputeOutput, Self::Error> {
        Ok(
            match self
                .compute_response_one_of
                .ok_or(anyhow!("could not find compute_response_one_of"))?
            {
                proto::channel::compute_response::ComputeResponseOneOf::Status(x) => {
                    ComputeOutput::Status {
                        status: x
                            .status
                            .ok_or(anyhow!("could not find status"))?
                            .try_into()?,
                    }
                }
                proto::channel::compute_response::ComputeResponseOneOf::Loaded(x) => {
                    ComputeOutput::Loaded { data: x.data }
                }
                proto::channel::compute_response::ComputeResponseOneOf::Completed(_) => {
                    ComputeOutput::Completed
                }
                proto::channel::compute_response::ComputeResponseOneOf::Submitted(_) => {
                    ComputeOutput::Submitted
                }
            },
        )
    }
}

impl TryFrom<ComputeOutput> for proto::channel::ComputeResponse {
    type Error = anyhow::Error;

    fn try_from(value: ComputeOutput) -> Result<Self, Self::Error> {
        match value {
            ComputeOutput::Status { status } => Ok(proto::channel::ComputeResponse {
                compute_response_one_of: Some(
                    proto::channel::compute_response::ComputeResponseOneOf::Status(
                        proto::channel::StatusResponse {
                            status: Some(status.try_into()?),
                        },
                    ),
                ),
            }),
            ComputeOutput::Loaded { data } => Ok(proto::channel::ComputeResponse {
                compute_response_one_of: Some(
                    proto::channel::compute_response::ComputeResponseOneOf::Loaded(
                        proto::channel::LoadedResponse { data },
                    ),
                ),
            }),
            ComputeOutput::Completed => Ok(proto::channel::ComputeResponse {
                compute_response_one_of: Some(
                    proto::channel::compute_response::ComputeResponseOneOf::Completed(
                        proto::channel::CompletedResponse {},
                    ),
                ),
            }),
            ComputeOutput::Submitted => Ok(proto::channel::ComputeResponse {
                compute_response_one_of: Some(
                    proto::channel::compute_response::ComputeResponseOneOf::Submitted(
                        proto::channel::SubmittedResponse {},
                    ),
                ),
            }),
        }
    }
}

impl TryInto<ModuleInfo> for proto::channel::ModuleInfo {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ModuleInfo, Self::Error> {
        Ok(ModuleInfo {
            name: self.name,
            version: self.version,
            modtype: ModuleType::from_str(&self.modtype)?,
        })
    }
}

impl TryFrom<ModuleInfo> for proto::channel::ModuleInfo {
    type Error = anyhow::Error;

    fn try_from(value: ModuleInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            version: value.version,
            modtype: value.modtype.to_string(),
        })
    }
}

impl TryInto<Symbol> for proto::channel::Symbol {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Symbol, Self::Error> {
        Ok(Symbol {
            module_info: self
                .module_info
                .ok_or(anyhow!("could not find module_info"))?
                .try_into()?,
            name: self.name,
        })
    }
}

impl TryFrom<Symbol> for proto::channel::Symbol {
    type Error = anyhow::Error;

    fn try_from(value: Symbol) -> Result<Self, Self::Error> {
        Ok(Self {
            module_info: Some(value.module_info.try_into()?),
            name: value.name,
        })
    }
}

impl TryInto<JobSpec> for proto::channel::JobSpec {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<JobSpec, Self::Error> {
        Ok(JobSpec {
            handle: self.handle,
            symbol: self
                .symbol
                .ok_or(anyhow!("could not find symbol"))?
                .try_into()?,
            input_handle: self.input_handle,
            output_handle: self.output_handle,
            ttl: self.ttl,
            extensions: self.extensions,
        })
    }
}

impl TryFrom<JobSpec> for proto::channel::JobSpec {
    type Error = anyhow::Error;

    fn try_from(value: JobSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            handle: value.handle,
            input_handle: value.input_handle,
            output_handle: value.output_handle,
            ttl: value.ttl,
            symbol: Some(value.symbol.try_into()?),
            extensions: value.extensions,
        })
    }
}

impl TryInto<StorageSpec> for proto::channel::StorageSpec {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<StorageSpec, Self::Error> {
        Ok(StorageSpec {
            handle: self.handle,
            data: self.data,
            ttl: self.ttl,
            extensions: self.extensions,
        })
    }
}

impl TryFrom<StorageSpec> for proto::channel::StorageSpec {
    type Error = anyhow::Error;

    fn try_from(value: StorageSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            handle: value.handle,
            data: value.data,
            ttl: value.ttl,
            extensions: value.extensions,
        })
    }
}

impl TryInto<JobStatusType> for proto::channel::JobStatusType {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<JobStatusType, Self::Error> {
        match self.job_status_type_one_of {
            Some(x) => match x {
                proto::channel::job_status_type::JobStatusTypeOneOf::Running(_) => {
                    Ok(JobStatusType::Running)
                }
                proto::channel::job_status_type::JobStatusTypeOneOf::Completed(_) => {
                    Ok(JobStatusType::Completed)
                }
                proto::channel::job_status_type::JobStatusTypeOneOf::Aborted(_) => {
                    Ok(JobStatusType::Aborted)
                }
                proto::channel::job_status_type::JobStatusTypeOneOf::ExpiredAt(x) => {
                    Ok(JobStatusType::ExpiredAt {
                        datetime: DateTime::<Utc>::from_naive_utc_and_offset(
                            NaiveDateTime::from_timestamp_opt(
                                x.datetime.ok_or(anyhow!("cannot find datetime"))?.seconds,
                                0,
                            )
                            .ok_or(anyhow!("failed to parse timestamp"))?,
                            Utc,
                        ),
                    })
                }
            },
            None => Err(anyhow!("cannot find job_status_type_one_of")),
        }
    }
}

impl TryFrom<JobStatusType> for proto::channel::JobStatusType {
    type Error = anyhow::Error;

    fn try_from(value: JobStatusType) -> Result<Self, Self::Error> {
        Ok(Self {
            job_status_type_one_of: match value {
                JobStatusType::Running => Some(
                    proto::channel::job_status_type::JobStatusTypeOneOf::Running(
                        proto::channel::job_status_type::Running {},
                    ),
                ),
                JobStatusType::Completed => Some(
                    proto::channel::job_status_type::JobStatusTypeOneOf::Completed(
                        proto::channel::job_status_type::Completed {},
                    ),
                ),
                JobStatusType::Aborted => Some(
                    proto::channel::job_status_type::JobStatusTypeOneOf::Aborted(
                        proto::channel::job_status_type::Aborted {},
                    ),
                ),
                JobStatusType::ExpiredAt { datetime } => Some(
                    proto::channel::job_status_type::JobStatusTypeOneOf::ExpiredAt(
                        proto::channel::job_status_type::ExpiredAt {
                            datetime: Some(prost_types::Timestamp {
                                seconds: datetime.timestamp(),
                                nanos: 0,
                            }),
                        },
                    ),
                ),
            },
        })
    }
}

impl TryInto<JobStatus> for proto::channel::JobStatus {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<JobStatus, Self::Error> {
        Ok(JobStatus {
            status: self
                .status
                .ok_or(anyhow!("could not find status"))?
                .try_into()?,
            extensions: self.extensions,
        })
    }
}

impl TryFrom<JobStatus> for proto::channel::JobStatus {
    type Error = anyhow::Error;

    fn try_from(value: JobStatus) -> Result<Self, Self::Error> {
        Ok(proto::channel::JobStatus {
            status: Some(value.status.try_into()?),
            extensions: value.extensions,
        })
    }
}
