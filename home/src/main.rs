use home::core::MessageId;
use home::core::Core;

pub fn main() {
    let mut core = Core::load("../cores/demo").unwrap();
    dbg!(&core);
    core.load_message(MessageId(0)).unwrap();
    core.load_message(MessageId(1)).unwrap();
    let id = core.add_message(b"all your base are belong to us").unwrap();
    core.flush().unwrap();
    dbg!(&core);
}
