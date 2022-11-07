from PyQt5 import QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import sys
from annotation import process, init_conn

#class for scrollable label
class ScrollableLabel(QScrollArea):
    #constructor
    def __init__(self, *args, **kwargs):
        QScrollArea.__init__(self, *args, **kwargs)
        widget = QWidget(self)
        #make widget resizable
        self.setWidgetResizable(True)
        self.setWidget(widget)
        #vertical box layout
        lay = QVBoxLayout(widget)
        #create label
        self.label = QLabel(widget)
        #set alignment to the text
        self.label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        #set font of text
        self.label.setFont(QFont('Arial', 14))
        #set style of widget
        self.label.setStyleSheet("border: 1px solid; border-color: black; background-color:white")
        #add label to layout
        lay.addWidget(self.label)

    #setText method
    def setText(self, text):
        #setting text to the label
        self.label.setText(text)

class MyWindow(QMainWindow):
    def __init__(self):
        #set windows
        super(MyWindow, self).__init__()
        self.setGeometry(1,42,1580,920)
        self.setWindowTitle("CZ4031 Project 2")

        #textbox for query
        self.queryTextbox = QTextEdit(self)
        #label to indicate which db name is the app currently connected to
        self.dbLabel = ScrollableLabel(self)
        #textbox for db
        self.dbTextbox = QTextEdit(self)
        # Button to run the algorithm
        self.sendButton = QtWidgets.QPushButton(self)
        #output text for query
        self.queryOutput = ScrollableLabel(self)
        #output text for annotation
        self.queryAnnotate = ScrollableLabel(self)
        #error message box
        self.error_dialog = QtWidgets.QErrorMessage()

        #initialise UI
        self.initUI()

        #db connection settings
        self.conn = None
        self.db = ''

    #initialise UI method
    def initUI(self):

        self.queryTextbox.move(25, 20)
        self.queryTextbox.resize(860, 300)
        self.queryTextbox.setPlaceholderText('Enter SQL Query Here:')
        self.queryTextbox.setFont(QFont('Arial', 14))

        self.dbLabel.move(900, 20)
        self.dbLabel.resize(500, 117)
        self.dbLabel.setText(f"Current DB Name: ")

        self.dbTextbox.move(900, 147)
        self.dbTextbox.resize(500, 50)
        self.dbTextbox.setPlaceholderText('Enter Database Name Here:')
        self.dbTextbox.setFont(QFont('Arial', 14))

        self.sendButton.move(900, 220)
        self.sendButton.resize(500, 100)
        self.sendButton.setText("Send Query")
        self.sendButton.setFont(QFont('Arial', 14))
        self.sendButton.clicked.connect(self.onClick)
     
        self.queryOutput.move(25, 333)
        self.queryOutput.resize(700, 567)
        self.queryOutput.setText("Output Query here:")
        self.queryOutput.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.queryOutput.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        self.queryAnnotate.move(735, 333)
        self.queryAnnotate.resize(800, 567)
        self.queryAnnotate.setText("Annotation here:")
        self.queryAnnotate.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.queryAnnotate.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        self.queryOutput.verticalScrollBar().valueChanged.connect(
            self.queryAnnotate.verticalScrollBar().setValue)
        self.queryAnnotate.verticalScrollBar().valueChanged.connect(
            self.queryOutput.verticalScrollBar().setValue)

    #click send button method
    def onClick(self):
        #db connection
        if self.db != self.dbTextbox.toPlainText():
            try:
                self.conn = init_conn(self.dbTextbox.toPlainText())
                self.db = self.dbTextbox.toPlainText()
                self.dbLabel.setText(f"Current DB Name: {self.db}")
            except Exception as e:
                self.error_dialog.showMessage(f"ERROR - {e}")
        #process query
        if self.conn is not None:
            try:
                query, annotation = process(self.conn, self.queryTextbox.toPlainText())
                self.queryOutput.setText('\n'.join(query))
                self.queryAnnotate.setText('\n'.join(annotation))
            except Exception as e:
                self.error_dialog.showMessage(f"ERROR - {e}")

def window():
    app = QApplication(sys.argv)
    win = MyWindow()

    win.show()
    sys.exit(app.exec_())

def main():
    window()

if __name__ == '__main__':
    main()
