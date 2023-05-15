from subprocess import Popen

def main():
    Popen(
        ['python', '-m', 'unittest', 'discover', '-s', 'test', '-p', '*.py', '-b']
    )