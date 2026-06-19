//! The projector ports: message → projection, session → scope, session → warm-up.

use mirrorlane_runtime::Step;

use crate::message::{ConversationId, MessageEnvelope};
use crate::projection::Projection;
use crate::routing::{RoutingDecision, RoutingHint};
use crate::scope::Scope;
use crate::skill::{Participant, SessionDevelopers, SkillContribution, SkillIndex, TopicOwnership};
use crate::warmup::WarmupDocument;

/// Maps a [`MessageEnvelope`] to a [`Projection`] — the message projector,
/// expressed as the runtime's generic [`Step`] specialized to projection.
///
/// `Projector` is a **named view** of `Step<In = MessageEnvelope, Out =
/// Projection>`: any such `Step` is automatically a `Projector` (blanket impl
/// below), and [`project`](Projector::project) is the domain-named entry point
/// that delegates to [`Step::run`], so existing call sites read in projection
/// terms. Implement [`Step`] — not `Projector` — for a concrete projector.
///
/// Implementations must be **deterministic and I/O-free**: the same message
/// always projects to the same result. This keeps replay deterministic. A real
/// SLM-backed projector wraps its own runtime and caches per input to preserve
/// this property.
pub trait Projector: Step<In = MessageEnvelope, Out = Projection> {
    /// Project a message. Defaults to [`Step::run`].
    fn project(&self, message: &MessageEnvelope) -> Projection {
        self.run(message)
    }
}

impl<T> Projector for T where T: Step<In = MessageEnvelope, Out = Projection> {}

/// Maps a session — a conversation and its [`Projection`]s — to a [`Scope`].
///
/// Like [`Projector`], implementations must be deterministic and I/O-free: the
/// same projections always produce the same scope.
pub trait ScopeProjector: Send + Sync {
    fn scope(&self, conversation: &ConversationId, projections: &[Projection]) -> Scope;
}

/// Maps a session — a conversation, its optional [`Scope`], and its
/// [`Projection`]s — to a [`WarmupDocument`].
///
/// Like the other ports, implementations must be deterministic and I/O-free: the
/// same inputs always produce the same document.
pub trait WarmupBuilder: Send + Sync {
    fn build(
        &self,
        conversation: &ConversationId,
        scope: Option<&Scope>,
        projections: &[Projection],
    ) -> WarmupDocument;
}

/// Builds a [`SkillIndex`] from authored contributions across the whole log.
///
/// Like the other ports, implementations must be deterministic and I/O-free: the
/// same contributions always produce the same index. Unlike the per-conversation
/// ports, this one aggregates globally.
pub trait SkillBuilder: Send + Sync {
    fn build(&self, contributions: &[SkillContribution]) -> SkillIndex;
}

/// Maps a [`Projection`] to a [`RoutingDecision`]: which consumer should receive
/// it, and why.
///
/// Like the other ports, implementations must be deterministic and I/O-free: the
/// same projection always routes the same way. A rule-based router applies
/// confidence escalation.
pub trait Router: Send + Sync {
    fn route(&self, projection: &Projection) -> (RoutingDecision, crate::routing::RoutingTrace);
}

/// Maps a [`Projection`] and the [`TopicOwnership`]s for its topics to a
/// [`RoutingHint`]: the ranked reviewer candidates and the best human to route
/// to.
///
/// Like the other ports, implementations must be deterministic and I/O-free: the
/// same projection and ownerships always produce the same hint. It takes the
/// ownerships directly rather than a store handle so the core computation stays
/// free of I/O ports.
pub trait RoutingHinter: Send + Sync {
    fn hint(&self, projection: &Projection, ownerships: &[TopicOwnership]) -> RoutingHint;
}

/// Maps a session — a conversation, its [`Participant`]s, and the
/// [`TopicOwnership`]s for the session's topics — to a [`SessionDevelopers`]: who
/// participated and which of the session's topics each one owns.
///
/// Like the other ports, implementations must be deterministic and I/O-free: the
/// same participants and ownerships always produce the same result. It takes the
/// ownerships directly rather than a store handle so the core computation stays
/// free of I/O ports.
pub trait DeveloperSnapshotBuilder: Send + Sync {
    fn build(
        &self,
        conversation: &ConversationId,
        participants: &[Participant],
        ownerships: &[TopicOwnership],
    ) -> SessionDevelopers;
}
