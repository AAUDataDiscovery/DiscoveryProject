use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::ptr::hash;
use crc32fast::Hasher;

pub(crate) fn hash_file(path: String) -> Result<u32 ,std::io::Error> {
    println!("{}", path);
    let mut hasher = Hasher::new();
    const CAP: usize = 1024 * 128;

    let mut file = File::open(path)?;

    let mut reader = BufReader::with_capacity(CAP, file);

    loop {
        let length = {
            let buffer = reader.fill_buf()?;
            hasher.update(buffer);
            buffer.len()
        };
        if length == 0 {
            break;
        }
        reader.consume(length);
    }

    Ok(hasher.finalize())
}


/*pub(crate) fn hash_file(path: String) -> Result<u32 ,std::io::Error> {
    println!("{}", path);
    let mut f = File::open(path)?;
    let mut buffer : [u8; 200] = [0; 200];

    read_first_10_bytes(&mut buffer[..100], &f).unwrap();
    read_last_10_bytes(&mut buffer[100..], &f).unwrap();

    let mut hasher = Hasher::new();
    hasher.update(&buffer[..]);
    Ok(hasher.finalize())
}

fn read_first_10_bytes(buf: &mut [u8], mut f: &File) -> Result<bool ,std::io::Error> {
    // read up to 10 bytes
    f.read(buf)?;
    Ok(true)
}

fn read_last_10_bytes(buf: &mut [u8], mut f: &File) -> Result<bool ,std::io::Error> {
    // read up to 10 bytes from the end of the file
    f.seek(SeekFrom::End(-100))?;
    f.read(buf)?;
    Ok(true)
}*/