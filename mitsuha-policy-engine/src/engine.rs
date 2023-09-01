use async_trait::async_trait;
use mitsuha_core::{channel::ComputeInput, errors::Error, types};

use crate::{Policy, PolicyEngine, Subject, Action};

pub struct StandardPolicyEngine;

#[async_trait]
impl PolicyEngine for StandardPolicyEngine {
    async fn evaluate(&self, input: &ComputeInput, policies: &Vec<Policy>) -> types::Result<bool> {
        let mut is_allowed: bool = false;

        for policy in policies {
            let (allow, ignore) = Self::evaluate_policy(input, policy)?;
            if !ignore {
                is_allowed = allow;
            }
        }

        Ok(is_allowed)
    }

    async fn contains(&self, parent: &Vec<Policy>, child: &Vec<Policy>) -> types::Result<bool> {
        for child_policy in child {
            let mut is_allowed: bool = false;

            for parent_policy in parent {
                let (allow, ignore) = Self::contains_policy(parent_policy, child_policy)?;
                if !ignore {
                    is_allowed = allow;
                }
            }

            if !is_allowed {
                return Ok(false);
            }
        }
        
        Ok(true)
    }
}

impl StandardPolicyEngine {
    fn handle_has_wildcard(handle_expr: &String) -> types::Result<bool> {
        let indices: Vec<_> = handle_expr.match_indices("*").collect();
        if indices.len() > 1 {
            return Err(Error::InvalidOperation { message: format!("invalid handle expression defined in policy, more than one wildcard is not allowed. handle expression: '{}'", handle_expr) });
        }

        if indices.len() == 1 && !handle_expr.ends_with("*") {
            return Err(Error::InvalidOperation { message: format!("invalid handle expression defined in policy, wildcard can only be used at the end of the expression. handle expression: '{}'", handle_expr) });
        }

        Ok(indices.len() == 1)
    }

    fn is_handle_match(handle_expr: &String, handle: &String) -> types::Result<bool> {
        if Self::handle_has_wildcard(handle_expr)? {
            return Ok(handle.starts_with(handle_expr.strip_suffix("*").unwrap()));
        }

        Ok(handle == handle_expr)
    }

    fn evaluate_policy(input: &ComputeInput, policy: &Policy) -> types::Result<(bool, bool)> {
        let mut allow: bool = false;
        let mut ignore: bool = true;

        match (input, policy) {
            (
                ComputeInput::Run { spec },
                Policy {
                    subject: Subject::RunJob { handle: handle_expr, ttl },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, &spec.handle)? {
                    ignore = false;
                }

                if ttl >= &spec.ttl {
                    allow = true;
                }   
            },
            (
                ComputeInput::Status { handle, .. },
                Policy {
                    subject: Subject::GetJobStatus { handle: handle_expr},
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            (
                ComputeInput::Extend { handle, ttl , .. },
                Policy {
                    subject: Subject::ExtendJob { handle: handle_expr, ttl: ttl_expr},
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }   
            },
            (
                ComputeInput::Abort { handle, .. },
                Policy {
                    subject: Subject::AbortJob { handle: handle_expr},
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            (
                ComputeInput::Store { spec },
                Policy {
                    subject: Subject::StoreBlob { handle: handle_expr, ttl },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, &spec.handle)? {
                    ignore = false;
                }

                if ttl >= &spec.ttl {
                    allow = true;
                }   
            },
            (
                ComputeInput::Load { handle, .. },
                Policy {
                    subject: Subject::LoadBlob { handle: handle_expr},
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            (
                ComputeInput::Persist { handle, ttl , .. },
                Policy {
                    subject: Subject::PersistBlob { handle: handle_expr, ttl: ttl_expr},
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }   
            },
            (
                ComputeInput::Clear { handle, .. },
                Policy {
                    subject: Subject::ClearBlob { handle: handle_expr},
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            _ => {}
        }

        if Action::Deny == policy.action {
            allow = false;
        }

        Ok((allow, ignore))
    }

    fn contains_policy(parent: &Policy, child: &Policy) -> types::Result<(bool, bool)> {
        if child.action == Action::Deny {
            return Ok((true, false));
        }

        let mut allow: bool = false;
        let mut ignore: bool = true;

        match (child, parent) {
            (
                Policy {
                    subject: Subject::RunJob { handle, ttl },
                    ..
                },
                Policy {
                    subject: Subject::RunJob { handle: handle_expr, ttl: ttl_expr },
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }   
            },
            (
                Policy {
                    subject: Subject::GetJobStatus { handle},
                    ..
                },
                Policy {
                    subject: Subject::GetJobStatus { handle: handle_expr},
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            (
                Policy {
                    subject: Subject::ExtendJob { handle, ttl},
                    ..
                },
                Policy {
                    subject: Subject::ExtendJob { handle: handle_expr, ttl: ttl_expr},
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }   
            },
            (
                Policy {
                    subject: Subject::AbortJob { handle},
                    ..
                },
                Policy {
                    subject: Subject::AbortJob { handle: handle_expr},
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            (
                Policy {
                    subject: Subject::StoreBlob { handle, ttl },
                    ..
                },
                Policy {
                    subject: Subject::StoreBlob { handle: handle_expr, ttl: ttl_expr },
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }   
            },
            (
                Policy {
                    subject: Subject::LoadBlob { handle},
                    ..
                },
                Policy {
                    subject: Subject::LoadBlob { handle: handle_expr},
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;
                
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            (
                Policy {
                    subject: Subject::PersistBlob { handle, ttl},
                    ..
                },
                Policy {
                    subject: Subject::PersistBlob { handle: handle_expr, ttl: ttl_expr},
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }   
            },
            (
                Policy {
                    subject: Subject::ClearBlob { handle},
                    ..
                },
                Policy {
                    subject: Subject::ClearBlob { handle: handle_expr},
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            },
            _ => {}
        }

        if Action::Deny == parent.action {
            allow = false;
        }

        Ok((allow, ignore))
    }
}

#[cfg(test)]
mod test {
    use mitsuha_core::channel::ComputeInput;

    use crate::{Policy, Action, Subject, PolicyEngine};

    use super::StandardPolicyEngine;

    static POLICY_ENGINE: StandardPolicyEngine = StandardPolicyEngine;

    #[tokio::test]
    async fn test_more_than_one_wildcard_handle_error() {
        let policy = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/*/*".to_string() }
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wildcard_not_at_end_error() {
        let policy = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/*/y".to_string() }
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wildcard_at_end_only() {
        let policy = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_no_wildcard() {
        let policy = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_deny() {
        let policy = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_allow_then_deny() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_deny_then_allow() {
        let policy_1 = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_deny_different_then_allow() {
        let policy_1 = Policy {
            action: Action::Deny,
            subject: Subject::LoadBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_deny_multi_allow_one() {
        let policy_1 = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Clear { handle: "job/myapp/x/y".to_string(), extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_allow_persist_ttl_negative() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::PersistBlob { handle: "job/myapp/x/*".to_string(), ttl: 100 }
        };

        let policies = vec![policy_1];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Persist { handle: "job/myapp/x/y".to_string(), ttl: 120, extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_allow_persist_ttl_positive() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::PersistBlob { handle: "job/myapp/x/*".to_string(), ttl: 100 }
        };

        let policies = vec![policy_1];

        let result = POLICY_ENGINE.evaluate(&ComputeInput::Persist { handle: "job/myapp/x/y".to_string(), ttl: 100, extensions: Default::default() }, &policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_allow_negative() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_allow_positive() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_deny() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_deny_reverse() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_deny_allow() {
        let policy_1 = Policy {
            action: Action::Deny,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_allow_one_many() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_3 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/z".to_string() }
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2, policy_3];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_allow_many_many() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/y/*".to_string() }
        };

        let policy_3 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_4 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/y/z".to_string() }
        };

        let parent_policies = vec![policy_1, policy_2];
        let child_policies = vec![policy_3, policy_4];


        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_contains_allow_allow_many_many_negative() {
        let policy_1 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/*".to_string() }
        };

        let policy_2 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/y/*".to_string() }
        };

        let policy_3 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/x/y".to_string() }
        };

        let policy_4 = Policy {
            action: Action::Allow,
            subject: Subject::ClearBlob { handle: "job/myapp/*".to_string() }
        };

        let parent_policies = vec![policy_1, policy_2];
        let child_policies = vec![policy_3, policy_4];

        let result = POLICY_ENGINE.contains(&parent_policies, &child_policies).await;
        
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}