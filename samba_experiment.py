from smb.SMBConnection import SMBConnection
import smbclient
from smbprotocol import Dialects
from smbprotocol.connection import Connection


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


def list_files_recursive(path, depth=0):
    for item in smbclient.listdir(path):
        item_path = f"{path}\\{item}"
        if item.is_dir(item_path):
            print("  " * depth + f"[D] {item}")
            list_files_recursive(item_path, depth + 1)
        else:
            print("  " * depth + f"[F] {item}")


def connect_to_samba_share():
    connection = Connection(1212, "192.168.10.1", port=139)
    connection.connect(Dialects.SMB_2_1_0)

    if connection.is_connected():
        session = connection.session_setup()
        tree = session.tree_connect("FRITZ.NAS")
        query_directory(tree, "/500GB/")
        tree.disconnect()
    else:
        print("Failed to connect to Samba share")

    connection.disconnect()


def connect() -> None:
    # c = smbclient.ClientConfig(username="mark", password="6bB3gv%2zeF4wxJ(")

    # share_path = "\\\\192-168-10-1\\500GB"
    share_path = "//192-168-10-1/500GB"

    # list_files_recursive(share_path)

    smbclient.register_session("192.168.10.1", username="mark", password="6bB3gv%2zeF4wxJ(", port=139)
    for each_file in smbclient.walk("//192-168-10-1/FRITZ.NAS/500GB"):  # listDir, walk, scandir
        print(each_file)



if __name__ == "__main__":
    # connect_to_samba_share()
    connect()
