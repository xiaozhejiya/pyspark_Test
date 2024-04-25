# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    result = ""
    for x in range(1, 10):
        for y in range(x, 0, -1):
            result = str(y) + result
        for y1 in range(9 - x):
            result = " " + result
        for z in range(x - 1, 0, -1):
            result = result + str(z)
        for z1 in range(9 - x):
            result = result + " "
        print(result)
        result = ""

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
