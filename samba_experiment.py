from smb.SMBConnection import SMBConnection


def connect_to_samba_share():
    conn = SMBConnection("mark", "6bB3gv%2zeF4wxJ(", "192.168.10.24", "192-168-10-1")
    conn.connect("192.168.10.1")
    if conn:
        print("Connected to Samba share")
        for file in conn.listPath("FRITZ.NAS", "/500GB/", pattern="*"):

            if file.isDirectory:
                print(file.filename + "/", file.create_time, file.file_size)
            else:
                print(file.filename, file.create_time, file.file_size)

    else:
        print("Failed to connect to Samba share")

    conn.close()


if __name__ == "__main__":
    connect_to_samba_share()
