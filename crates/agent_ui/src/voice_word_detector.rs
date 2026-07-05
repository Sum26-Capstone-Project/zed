use agent_voice_detector::{DetectorEvent, start_detector};
use futures::{StreamExt as _, channel::mpsc};
use gpui::{App, AppContext as _, Context, Entity, EventEmitter, Global, Task};

/// A single, process-wide start/end word detector. It runs one microphone
/// capture + websocket session for all of Zed and re-emits each recognized
/// phrase as a [`DetectorEvent`]. Views that want to react (e.g. the voice input
/// button) subscribe to the shared entity returned by [`Self::global`] instead
/// of starting their own detector.
pub struct VoiceWordDetector {
    _task: Task<()>,
}

struct GlobalVoiceWordDetector(Entity<VoiceWordDetector>);

impl Global for GlobalVoiceWordDetector {}

impl EventEmitter<DetectorEvent> for VoiceWordDetector {}

impl VoiceWordDetector {
    /// Returns the process-wide detector, creating and starting it on first use.
    /// Subscribe to the returned entity to receive [`DetectorEvent`]s.
    pub fn global(cx: &mut App) -> Entity<Self> {
        if let Some(global) = cx.try_global::<GlobalVoiceWordDetector>() {
            return global.0.clone();
        }
        let detector = cx.new(Self::new);
        cx.set_global(GlobalVoiceWordDetector(detector.clone()));
        detector
    }

    fn new(cx: &mut Context<Self>) -> Self {
        let (event_tx, mut event_rx) = mpsc::unbounded();
        let detector_task = start_detector(cx.background_executor().clone(), event_tx);

        let task = cx.spawn(async move |this, cx| {
            // Hold the detector task so it lives as long as this entity does.
            let _detector_task = detector_task;
            while let Some(event) = event_rx.next().await {
                if this.update(cx, |_this, cx| cx.emit(event)).is_err() {
                    break;
                }
            }
        });

        Self { _task: task }
    }
}
