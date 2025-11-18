use ceptra::consensus_core::security::{Certificate, SerialNumber, SpiffeId};
use ceptra::{CepRole, CepSecurityMaterial, SecurityPolicyError, WorkloadDescriptor};
use std::time::{Duration, Instant};

#[test]
fn grants_roles_and_records_workloads() {
    let now = Instant::now();
    let cert = sample_certificate(now);
    let mut material = CepSecurityMaterial::new("spiffe://example.org/node-1", cert);
    material.grant_roles([CepRole::Operator, CepRole::Admin]);
    material.annotate_workload(WorkloadDescriptor::bundle("bundle-7"));
    material.annotate_workload(WorkloadDescriptor::lane_domain("lane-a"));

    assert!(material.roles().contains(&CepRole::Operator));
    assert!(material.workloads().bundles().contains("bundle-7"));
    assert!(material
        .authorize_workload(&WorkloadDescriptor::bundle("bundle-7"))
        .is_ok());
    assert!(material
        .authorize_workload(&WorkloadDescriptor::lane_domain("lane-a"))
        .is_ok());
    let err = material
        .authorize_workload(&WorkloadDescriptor::schema("schema-42"))
        .unwrap_err();
    assert!(matches!(
        err,
        SecurityPolicyError::WorkloadNotAllowed { .. }
    ));
}

#[test]
fn readyz_audit_enforces_observer_role() {
    let now = Instant::now();
    let cert = sample_certificate(now);
    let mut material = CepSecurityMaterial::new("spiffe://example.org/observer", cert);
    material.grant_role(CepRole::Ingest);

    let err = material.audit_readyz_access(now).unwrap_err();
    assert!(matches!(err, SecurityPolicyError::MissingRole { .. }));

    material.grant_role(CepRole::Observer);
    material
        .audit_readyz_access(now)
        .expect("observer role passes");
}

#[test]
fn ingest_audit_checks_certificate_validity() {
    let now = Instant::now();
    let mut cert = sample_certificate(now);
    cert.valid_until = now - Duration::from_secs(1);
    let mut material = CepSecurityMaterial::new("spiffe://example.org/ingest", cert);
    material.grant_role(CepRole::Ingest);

    let err = material.audit_ingest_access(now).unwrap_err();
    assert!(matches!(
        err,
        SecurityPolicyError::CertificateExpired { .. }
    ));

    let future = now + Duration::from_secs(120);
    let mut future_cert = sample_certificate(future);
    future_cert.valid_from = future + Duration::from_secs(5);
    let mut future_material = CepSecurityMaterial::new("spiffe://example.org/future", future_cert);
    future_material.grant_role(CepRole::Ingest);
    let err = future_material.audit_ingest_access(future).unwrap_err();
    assert!(matches!(
        err,
        SecurityPolicyError::CertificateNotYetValid { .. }
    ));
}

fn sample_certificate(now: Instant) -> Certificate {
    Certificate {
        spiffe_id: SpiffeId::parse("spiffe://example.org/node").unwrap(),
        serial: SerialNumber::from_u64(1),
        valid_from: now - Duration::from_secs(60),
        valid_until: now + Duration::from_secs(60),
    }
}
