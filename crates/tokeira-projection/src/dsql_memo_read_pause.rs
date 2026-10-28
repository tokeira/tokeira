//! Namespace-scoped synchronization of live list reads and concurrent memo writes.
//! A read holds after its snapshot page SELECT and before joining memo parts.

use std::sync::Mutex;

use tokeira_types::NamespaceId;
use tokio::sync::oneshot;

struct Pause {
    namespace: NamespaceId,
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

static PAUSES: Mutex<Vec<Pause>> = Mutex::new(Vec::new());

pub(super) fn arm(namespace: NamespaceId) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (reached, wait) = oneshot::channel();
    let (resume, release) = oneshot::channel();
    PAUSES.lock().expect("pause lock poisoned").push(Pause {
        namespace,
        reached,
        resume: release,
    });
    (wait, resume)
}

pub(super) async fn reached(namespace: NamespaceId) {
    let pause = {
        let mut pauses = PAUSES.lock().expect("pause lock poisoned");
        pauses
            .iter()
            .position(|pause| pause.namespace == namespace)
            .map(|index| pauses.remove(index))
    };
    if let Some(pause) = pause {
        let _ = pause.reached.send(());
        let _ = pause.resume.await;
    }
}
