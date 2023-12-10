use async_trait::async_trait;
use mitsuha_core::{errors::Error, types};
use mitsuha_core_types::channel::ComputeInput;

use crate::{Action, Permission, Policy, PolicyEngine};

/// A standard implementation of [PolicyEngine]
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
    /// Check if the handle expression has a wildcard character
    ///
    /// ### Arguments
    ///
    /// * `handle_expr` - The handle expression
    ///
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

    /// Check if a handle expression covers a handle
    ///
    /// ### Arguments
    ///
    /// * `handle_expr` - The handle expression
    /// * `handle` - The handle
    ///
    fn is_handle_match(handle_expr: &String, handle: &String) -> types::Result<bool> {
        if Self::handle_has_wildcard(handle_expr)? {
            return Ok(handle.starts_with(handle_expr.strip_suffix("*").unwrap()));
        }

        Ok(handle == handle_expr)
    }

    /// Evaluate a single policy against a [ComputeInput]
    ///
    /// ### Arguments
    ///
    /// * `input` - The [ComputeInput]
    /// * `policy` - The [Policy]
    ///
    fn evaluate_policy(input: &ComputeInput, policy: &Policy) -> types::Result<(bool, bool)> {
        let mut allow: bool = false;
        let mut ignore: bool = true;

        match (input, policy) {
            (
                ComputeInput::Run { spec },
                Policy {
                    action:
                        Action::RunJob {
                            handle: handle_expr,
                            ttl,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, &spec.handle)? {
                    ignore = false;
                }

                if ttl >= &spec.ttl {
                    allow = true;
                }
            }
            (
                ComputeInput::Status { handle, .. },
                Policy {
                    action:
                        Action::GetJobStatus {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            (
                ComputeInput::Extend { handle, ttl, .. },
                Policy {
                    action:
                        Action::ExtendJob {
                            handle: handle_expr,
                            ttl: ttl_expr,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }
            }
            (
                ComputeInput::Abort { handle, .. },
                Policy {
                    action:
                        Action::AbortJob {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            (
                ComputeInput::Store { spec },
                Policy {
                    action:
                        Action::StoreBlob {
                            handle: handle_expr,
                            ttl,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, &spec.handle)? {
                    ignore = false;
                }

                if ttl >= &spec.ttl {
                    allow = true;
                }
            }
            (
                ComputeInput::Load { handle, .. },
                Policy {
                    action:
                        Action::LoadBlob {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            (
                ComputeInput::Persist { handle, ttl, .. },
                Policy {
                    action:
                        Action::PersistBlob {
                            handle: handle_expr,
                            ttl: ttl_expr,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                if ttl_expr >= ttl {
                    allow = true;
                }
            }
            (
                ComputeInput::Clear { handle, .. },
                Policy {
                    action:
                        Action::ClearBlob {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            _ => {}
        }

        if Permission::Deny == policy.permission {
            allow = false;
        }

        Ok((allow, ignore))
    }

    /// Check if a policy is a superset of another
    ///
    /// ### Arguments
    ///
    /// * `parent` - The potential superset [Policy]
    /// * `child` - The potential subset [Policy]
    fn contains_policy(parent: &Policy, child: &Policy) -> types::Result<(bool, bool)> {
        if child.permission == Permission::Deny {
            return Ok((true, false));
        }

        let mut allow: bool = false;
        let mut ignore: bool = true;

        match (child, parent) {
            (
                Policy {
                    action: Action::RunJob { handle, ttl },
                    ..
                },
                Policy {
                    action:
                        Action::RunJob {
                            handle: handle_expr,
                            ttl: ttl_expr,
                        },
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
            }
            (
                Policy {
                    action: Action::GetJobStatus { handle },
                    ..
                },
                Policy {
                    action:
                        Action::GetJobStatus {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            (
                Policy {
                    action: Action::ExtendJob { handle, ttl },
                    ..
                },
                Policy {
                    action:
                        Action::ExtendJob {
                            handle: handle_expr,
                            ttl: ttl_expr,
                        },
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
            }
            (
                Policy {
                    action: Action::AbortJob { handle },
                    ..
                },
                Policy {
                    action:
                        Action::AbortJob {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            (
                Policy {
                    action: Action::StoreBlob { handle, ttl },
                    ..
                },
                Policy {
                    action:
                        Action::StoreBlob {
                            handle: handle_expr,
                            ttl: ttl_expr,
                        },
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
            }
            (
                Policy {
                    action: Action::LoadBlob { handle },
                    ..
                },
                Policy {
                    action:
                        Action::LoadBlob {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            (
                Policy {
                    action: Action::PersistBlob { handle, ttl },
                    ..
                },
                Policy {
                    action:
                        Action::PersistBlob {
                            handle: handle_expr,
                            ttl: ttl_expr,
                        },
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
            }
            (
                Policy {
                    action: Action::ClearBlob { handle },
                    ..
                },
                Policy {
                    action:
                        Action::ClearBlob {
                            handle: handle_expr,
                        },
                    ..
                },
            ) => {
                Self::handle_has_wildcard(handle)?;

                if Self::is_handle_match(handle_expr, handle)? {
                    ignore = false;
                }

                allow = true;
            }
            _ => {}
        }

        if Permission::Deny == parent.permission {
            allow = false;
        }

        Ok((allow, ignore))
    }
}

#[cfg(test)]
mod test {
    use mitsuha_core_types::channel::ComputeInput;

    use crate::{Action, Permission, Policy, PolicyEngine};

    use super::StandardPolicyEngine;

    static POLICY_ENGINE: StandardPolicyEngine = StandardPolicyEngine;

    /// Check if we throw an error when a handle expression has more than one wildcards in it
    #[tokio::test]
    async fn test_more_than_one_wildcard_handle_error() {
        let policy = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/*/*".to_string(),
            },
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_err());
    }

    /// Check if we throw an error when a handle expression has a wildcard but not at the end
    #[tokio::test]
    async fn test_wildcard_not_at_end_error() {
        let policy = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/*/y".to_string(),
            },
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_err());
    }

    /// Check if we allow wildcards at the end of a handle expression
    #[tokio::test]
    async fn test_wildcard_at_end_only() {
        let policy = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Check if we allow no wildcards in a handle expression
    #[tokio::test]
    async fn test_no_wildcard() {
        let policy = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test policy denial
    #[tokio::test]
    async fn test_deny() {
        let policy = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policies = vec![policy];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    /// Test allowing a action with a wildcard expression and then denying a subset of the former
    #[tokio::test]
    async fn test_allow_then_deny() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    /// Test if we allow overriding the permissions defined on a specific action
    #[tokio::test]
    async fn test_deny_then_allow() {
        let policy_1 = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test if there is no overlap between actions
    #[tokio::test]
    async fn test_deny_different_then_allow() {
        let policy_1 = Policy {
            permission: Permission::Deny,
            action: Action::LoadBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test denying a wildcard handle expression and allowing a subset
    #[tokio::test]
    async fn test_deny_multi_allow_one() {
        let policy_1 = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policies = vec![policy_1, policy_2];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Clear {
                    handle: "job/myapp/x/y".to_string(),
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test if we deny an action when ttl is beyond the maximum limit
    #[tokio::test]
    async fn test_allow_persist_ttl_negative() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::PersistBlob {
                handle: "job/myapp/x/*".to_string(),
                ttl: 100,
            },
        };

        let policies = vec![policy_1];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Persist {
                    handle: "job/myapp/x/y".to_string(),
                    ttl: 120,
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    /// Test if we allow an action when the ttl is within the limits
    #[tokio::test]
    async fn test_allow_persist_ttl_positive() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::PersistBlob {
                handle: "job/myapp/x/*".to_string(),
                ttl: 100,
            },
        };

        let policies = vec![policy_1];

        let result = POLICY_ENGINE
            .evaluate(
                &ComputeInput::Persist {
                    handle: "job/myapp/x/y".to_string(),
                    ttl: 100,
                    extensions: Default::default(),
                },
                &policies,
            )
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test if superset evaluation failure
    #[tokio::test]
    async fn test_contains_allow_allow_negative() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    /// Test superset evaluation success
    #[tokio::test]
    async fn test_contains_allow_allow_positive() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test allow-deny superset evaluation
    #[tokio::test]
    async fn test_contains_allow_deny() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test allow-deny superset evaluation
    #[tokio::test]
    async fn test_contains_allow_deny_reverse() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test deny-allow superset evaluation failure
    #[tokio::test]
    async fn test_contains_deny_allow() {
        let policy_1 = Policy {
            permission: Permission::Deny,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    /// Test allow - allow(2) superset evaluation
    #[tokio::test]
    async fn test_contains_allow_allow_one_many() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_3 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/z".to_string(),
            },
        };

        let parent_policies = vec![policy_1];
        let child_policies = vec![policy_2, policy_3];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test allow(2) - allow(2) superset evaluation
    #[tokio::test]
    async fn test_contains_allow_allow_many_many() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/y/*".to_string(),
            },
        };

        let policy_3 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_4 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/y/z".to_string(),
            },
        };

        let parent_policies = vec![policy_1, policy_2];
        let child_policies = vec![policy_3, policy_4];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    /// Test allow(2) - allow(2) superset evaluation failure
    #[tokio::test]
    async fn test_contains_allow_allow_many_many_negative() {
        let policy_1 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/*".to_string(),
            },
        };

        let policy_2 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/y/*".to_string(),
            },
        };

        let policy_3 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/x/y".to_string(),
            },
        };

        let policy_4 = Policy {
            permission: Permission::Allow,
            action: Action::ClearBlob {
                handle: "job/myapp/*".to_string(),
            },
        };

        let parent_policies = vec![policy_1, policy_2];
        let child_policies = vec![policy_3, policy_4];

        let result = POLICY_ENGINE
            .contains(&parent_policies, &child_policies)
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
