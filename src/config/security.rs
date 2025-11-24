use clustor::security::Certificate;
use std::collections::BTreeSet;
use std::fmt;
use std::time::Instant;
use thiserror::Error;

/// CEP-specific logical roles layered atop the Clustor security substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CepRole {
    Ingest,
    Observer,
    Operator,
    Admin,
}

impl CepRole {
    /// Returns the canonical string label used in manifests and audit logs.
    pub fn as_str(self) -> &'static str {
        match self {
            CepRole::Ingest => "ingest",
            CepRole::Observer => "observer",
            CepRole::Operator => "operator",
            CepRole::Admin => "admin",
        }
    }
}

impl fmt::Display for CepRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Descriptor for a CEP workload (bundle + optional lane domain + schema identifiers).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkloadDescriptor {
    pub bundle_id: Option<String>,
    pub lane_domain: Option<String>,
    pub schema_id: Option<String>,
}

impl WorkloadDescriptor {
    /// Creates a descriptor from optional components.
    pub fn new(
        bundle_id: Option<String>,
        lane_domain: Option<String>,
        schema_id: Option<String>,
    ) -> Self {
        Self {
            bundle_id,
            lane_domain,
            schema_id,
        }
    }

    /// Shortcut for a descriptor that targets a specific bundle.
    pub fn bundle(bundle_id: impl Into<String>) -> Self {
        Self {
            bundle_id: Some(bundle_id.into()),
            ..Self::default()
        }
    }

    /// Shortcut for a descriptor bound to a lane domain.
    pub fn lane_domain(lane_domain: impl Into<String>) -> Self {
        Self {
            lane_domain: Some(lane_domain.into()),
            ..Self::default()
        }
    }

    /// Shortcut for a descriptor targeting a schema ID.
    pub fn schema(schema_id: impl Into<String>) -> Self {
        Self {
            schema_id: Some(schema_id.into()),
            ..Self::default()
        }
    }

    /// True when no bundle, lane domain, or schema was specified.
    pub fn is_empty(&self) -> bool {
        self.bundle_id.is_none() && self.lane_domain.is_none() && self.schema_id.is_none()
    }
}

/// Workload allow-list tracked per node/principal.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WorkloadAssignments {
    bundles: BTreeSet<String>,
    lane_domains: BTreeSet<String>,
    schema_ids: BTreeSet<String>,
}

impl WorkloadAssignments {
    /// Adds a bundle identifier to the allow-list.
    pub fn allow_bundle(&mut self, bundle_id: impl Into<String>) {
        self.bundles.insert(bundle_id.into());
    }

    /// Adds a lane-domain identifier to the allow-list.
    pub fn allow_lane_domain(&mut self, lane_domain: impl Into<String>) {
        self.lane_domains.insert(lane_domain.into());
    }

    /// Adds a schema identifier to the allow-list.
    pub fn allow_schema(&mut self, schema_id: impl Into<String>) {
        self.schema_ids.insert(schema_id.into());
    }

    /// Records all components of the provided descriptor.
    pub fn record(&mut self, workload: &WorkloadDescriptor) {
        if let Some(bundle) = &workload.bundle_id {
            self.allow_bundle(bundle.clone());
        }
        if let Some(domain) = &workload.lane_domain {
            self.allow_lane_domain(domain.clone());
        }
        if let Some(schema) = &workload.schema_id {
            self.allow_schema(schema.clone());
        }
    }

    /// True when the descriptor falls within the recorded allow-list.
    pub fn allows(&self, workload: &WorkloadDescriptor) -> bool {
        if workload.is_empty() {
            return true;
        }
        if let Some(bundle) = &workload.bundle_id {
            if !self.bundles.contains(bundle) {
                return false;
            }
        }
        if let Some(domain) = &workload.lane_domain {
            if !self.lane_domains.contains(domain) {
                return false;
            }
        }
        if let Some(schema) = &workload.schema_id {
            if !self.schema_ids.contains(schema) {
                return false;
            }
        }
        true
    }

    /// Returns the allowed bundle identifiers.
    pub fn bundles(&self) -> &BTreeSet<String> {
        &self.bundles
    }

    /// Returns the allowed lane-domain identifiers.
    pub fn lane_domains(&self) -> &BTreeSet<String> {
        &self.lane_domains
    }

    /// Returns the allowed schema identifiers.
    pub fn schema_ids(&self) -> &BTreeSet<String> {
        &self.schema_ids
    }
}

/// Errors returned when security audits fail.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SecurityPolicyError {
    #[error("certificate for {subject} not yet valid")]
    CertificateNotYetValid { subject: String },
    #[error("certificate for {subject} expired")]
    CertificateExpired { subject: String },
    #[error("principal {subject} missing required role {role}")]
    MissingRole { subject: String, role: CepRole },
    #[error("principal {subject} not permitted to host workload {workload:?}")]
    WorkloadNotAllowed {
        subject: String,
        workload: WorkloadDescriptor,
    },
}

/// CEP binding for a Clustor-issued SecurityMaterial.
#[derive(Debug, Clone)]
pub struct CepSecurityMaterial {
    subject: String,
    certificate: Certificate,
    roles: BTreeSet<CepRole>,
    workloads: WorkloadAssignments,
}

impl CepSecurityMaterial {
    /// Creates a binding anchored to the provided subject/certificate.
    pub fn new(subject: impl Into<String>, certificate: Certificate) -> Self {
        Self {
            subject: subject.into(),
            certificate,
            roles: BTreeSet::new(),
            workloads: WorkloadAssignments::default(),
        }
    }

    /// Returns the principal identifier (typically the SPIFFE ID).
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the underlying certificate metadata.
    pub fn certificate(&self) -> &Certificate {
        &self.certificate
    }

    /// Returns the recorded workload allow-list.
    pub fn workloads(&self) -> &WorkloadAssignments {
        &self.workloads
    }

    /// Grants the provided role to the principal.
    pub fn grant_role(&mut self, role: CepRole) {
        self.roles.insert(role);
    }

    /// Grants multiple roles at once.
    pub fn grant_roles(&mut self, roles: impl IntoIterator<Item = CepRole>) {
        for role in roles {
            self.grant_role(role);
        }
    }

    /// Records a workload descriptor alongside the certificate.
    pub fn annotate_workload(&mut self, workload: WorkloadDescriptor) {
        self.workloads.record(&workload);
    }

    /// Returns the assigned role set.
    pub fn roles(&self) -> &BTreeSet<CepRole> {
        &self.roles
    }

    /// Ensures the workload descriptor is allowed for this principal.
    pub fn authorize_workload(
        &self,
        workload: &WorkloadDescriptor,
    ) -> Result<(), SecurityPolicyError> {
        if self.workloads.allows(workload) {
            Ok(())
        } else {
            Err(SecurityPolicyError::WorkloadNotAllowed {
                subject: self.subject.clone(),
                workload: workload.clone(),
            })
        }
    }

    /// Audits `/readyz` access (mTLS validity + observer role).
    pub fn audit_readyz_access(&self, now: Instant) -> Result<(), SecurityPolicyError> {
        self.ensure_certificate_valid(now)?;
        self.ensure_role(CepRole::Observer)
    }

    /// Audits ingest RPC access (mTLS validity + ingest role).
    pub fn audit_ingest_access(&self, now: Instant) -> Result<(), SecurityPolicyError> {
        self.ensure_certificate_valid(now)?;
        self.ensure_role(CepRole::Ingest)
    }

    fn ensure_role(&self, role: CepRole) -> Result<(), SecurityPolicyError> {
        if self.roles.contains(&role) {
            Ok(())
        } else {
            Err(SecurityPolicyError::MissingRole {
                subject: self.subject.clone(),
                role,
            })
        }
    }

    fn ensure_certificate_valid(&self, now: Instant) -> Result<(), SecurityPolicyError> {
        if now < self.certificate.valid_from {
            return Err(SecurityPolicyError::CertificateNotYetValid {
                subject: self.subject.clone(),
            });
        }
        if now > self.certificate.valid_until {
            return Err(SecurityPolicyError::CertificateExpired {
                subject: self.subject.clone(),
            });
        }
        Ok(())
    }
}
