pub mod composer;
pub mod notifier;

pub use composer::{
    AttentionComposeOutput, AttentionPulseKind, SignalComposer, SignalComposerConfig,
    SignalComposerInput,
};
pub use notifier::{AttentionNotifierConfig, AttentionNotifierDecision};
