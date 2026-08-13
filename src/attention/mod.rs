pub mod composer;
pub mod notifier;
pub mod persist;
pub mod rank;

pub use composer::{
    AttentionComposeOutput, AttentionPulseKind, SignalComposer, SignalComposerConfig,
    SignalComposerInput,
};
pub use notifier::{AttentionNotifierConfig, AttentionNotifierDecision};
pub use persist::persist_event_stream_attention;
pub use rank::{
    apply_inbox_cursor, attention_signal_from_kernel_event, event_stream_signal_id,
    rank_attention_inbox, signal_matches_inbox_filters, EVENT_STREAM_VIEW,
};
