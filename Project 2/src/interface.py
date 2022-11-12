from PyQt5 import QtWidgets
from PyQt5 import QtGui
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import sys
from annotation import *

#class for popup result window
class ResultWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowIcon(QtGui.QIcon('greendot.png'))
        self.setWindowTitle('Your Query Result!')
        self.setGeometry(27,290,1770,720)

        self.query = QLabel(self)
        self.lbloutput = QLabel(self)
        self.lblannotate = QLabel(self)
        self.lblAQP = QLabel(self)
         
        self.queryOutput = ScrollableLabel(self)
        self.queryAnnotate = ScrollableLabel(self)
        self.queryAQP = ScrollableLabel(self)

        self.query.move(25, 20)
        self.query.resize(1400, 100)
        self.query.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.query.setFont(QFont('Arial', 14))

        self.lbloutput.move(25,117)
        self.lbloutput.setText("QEP Tokenize Components:")
        self.lbloutput.setFont(QFont('Arial', 14))

        self.lblannotate.move(535,117)
        self.lblannotate.setText("QEP Corresponding Annotations:")
        self.lblannotate.setFont(QFont('Arial', 14))

        self.lblAQP.move(1145,117)
        self.lblAQP.setText("AQP:")
        self.lblAQP.setFont(QFont('Arial', 14))

        self.queryOutput.move(25, 150)
        self.queryOutput.resize(500, 560)
        self.queryOutput.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.queryOutput.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        self.queryAnnotate.move(535, 150)
        self.queryAnnotate.resize(600, 560)
        self.queryAnnotate.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.queryAnnotate.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        self.queryAQP.move(1145, 150)
        self.queryAQP.resize(600, 560)
        self.queryAQP.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.queryAQP.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

    def displayInfo(self):
        self.show()

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
        self.setWindowIcon(QtGui.QIcon('cutecat.png'))
        self.setGeometry(1,42,1070,350)
        self.setWindowTitle("CZ4031 Project 2")
        self.result = ResultWindow()
        #textbox for query
        self.queryTextbox = QTextEdit(self)
        #label to indicate which db name is the app currently connected to
        self.dbLabel = ScrollableLabel(self)
        #textbox for db
        self.dbTextbox = QTextEdit(self)
        # Button to run the algorithm
        self.sendButton = QtWidgets.QPushButton(self)
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
        self.queryTextbox.resize(600, 300)
        self.queryTextbox.setPlaceholderText('Enter SQL Query Here:')
        self.queryTextbox.setFont(QFont('Arial', 14))

        self.dbLabel.move(640, 20)
        self.dbLabel.resize(400, 117)
        self.dbLabel.setText(f"Current DB Name: ")

        self.dbTextbox.move(640, 147)
        self.dbTextbox.resize(400, 50)
        self.dbTextbox.setPlaceholderText('Enter Database Name Here:')
        self.dbTextbox.setFont(QFont('Arial', 14))

        self.sendButton.move(640, 220)
        self.sendButton.resize(400, 100)
        self.sendButton.setText("Send Query")
        self.sendButton.setFont(QFont('Arial', 14))
        self.sendButton.clicked.connect(self.onClick)

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
                query, annotation,annotation2= process(self.conn, self.queryTextbox.toPlainText())
                print("IS ERROR HERE")
                #print(annotation2)
                
                self.result.queryOutput.setText('\n'.join(query))
                self.result.queryAnnotate.setText('\n'.join(annotation))
                self.result.queryAQP.setText('\n'.join(annotation2))
                #self.result.queryAQP.setText(TODO)
                #query = preprocess_query_string(query)

                q = "Query Entered: " + self.queryTextbox.toPlainText()
                self.result.query.setText(q)
                self.result.displayInfo()
            except Exception as e:
                self.error_dialog.showMessage(f"ERROR - {e}")
    
def window():
    app = QApplication(sys.argv)
    demo = MyWindow()
    demo.show()
    sys.exit(app.exec_())

def main():
    window()

if __name__ == '__main__':
    main()
