import os
import subprocess


def copy_by_putty(folder, filename, destination_folder, batch_folder, batch_file, args_file):

    """SFTP extraction of a file from sftp using putty (requiered putty install - psftp method)

        Parameters
        -----------
        folder: str
            folder of the sftp
        filename: str
            name of the file targeted on the sftp
        destination_folder: str
            local name of the folder where the file needs to be copied
        batch_folder: str
            path of the folder where the batch file file is stored (needs to be the same for args_file)
        batch_file: str
            name of the file where the batch file for putty is store (needs to include credentials and conenction for sftp)
        args_file: str
            get method for sftp

    """



    if os.path.isfile(batch_folder + args_file):
        os.remove(batch_folder + args_file)

    f = open(batch_folder + args_file.split(".")[0] + ".txt", "w+")
    f.write("get {}{} {}{}".format(folder, filename, destination_folder, filename))
    f.close()

    os.rename(batch_folder + args_file.split(".")[0] + ".txt", batch_folder + args_file.split(".")[0] + ".sftp")

    subprocess.call([batch_folder + batch_file])