use core::pipes;
use core::pipes::Select2;

use core::task;

fn main() {
    let (port, chan) = pipes::stream();
    let (port2, chan2) = pipes::stream();

    chan.send(~"Hello");
    chan2.send(~"world");

    loop {
        let msg = match (port, port2).select() {
            Left(a) => {
                a
            },
            Right(b) => {
                b
            },
        };
        io::println(msg);
    }
}
