import sys
import os
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import csv
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QSpinBox, QPushButton, QGroupBox,
    QMessageBox, QSplitter, QSizePolicy
)
from PyQt5.QtCore import Qt

from db_connection import main, test_names, run_postgres, run_mongo, run_cassandra, run_mariadb, load_database_credentials

class PlotCanvas(FigureCanvas):
    def __init__(self, parent=None, width=10, height=6, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        super(PlotCanvas, self).__init__(self.fig)
        self.setParent(parent)
        FigureCanvas.setSizePolicy(self,
                                  QSizePolicy.Expanding,
                                  QSizePolicy.Expanding)
        FigureCanvas.updateGeometry(self)

    def plot_results(self, test_name):
        self.axes.clear()
        folder_path = f"./results/{test_name}/"

        if not os.path.exists(folder_path):
            self.axes.set_title("No results available")
            self.draw()
            return False

        all_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

        if not all_files:
            self.axes.set_title("No results available")
            self.draw()
            return False

        for filename in all_files:
            full_path = os.path.join(folder_path, filename)
            db_label = filename.replace(".csv", "")

            x_vals = []
            y_vals = []

            with open(full_path, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    try:
                        x_vals.append(int(row[0]))
                        y_vals.append(float(row[1]))
                    except (ValueError, IndexError):
                        continue

            if not x_vals or not y_vals:
                continue

            sorted_pairs = sorted(zip(x_vals, y_vals), key=lambda pair: pair[0])
            if sorted_pairs:
                x_vals, y_vals = zip(*sorted_pairs)
                self.axes.plot(x_vals, y_vals, marker='o', label=db_label)

        self.axes.set_xlabel("Number of Records")
        self.axes.set_ylabel("Execution Time (s)")
        self.axes.set_title(f"Execution Time Comparison for Test: {test_name}")
        self.axes.legend()
        self.axes.grid(True)

        self.fig.tight_layout()
        self.draw()
        return True

class DatabaseBenchmarkGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Database Benchmark Tool")
        self.setGeometry(100, 100, 1000, 600)

        self.setStyleSheet("""
            QMainWindow {
                background-color: #232629;
            }
            QWidget {
                background-color: #232629;
                color: #f0f0f0;
                font-size: 14px;
            }
            QGroupBox {
                border: 2px solid #444;
                border-radius: 8px;
                margin-top: 10px;
                background-color: #282c34;
                font-weight: bold;
                color: #f0f0f0;
            }
            QGroupBox:title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                padding: 0 3px;
            }
            QLabel {
                color: #f0f0f0;
            }
            QComboBox, QSpinBox {
                background-color: #353941;
                color: #f0f0f0;
                border: 1px solid #555;
                border-radius: 5px;
                padding: 4px 8px;
                font-size: 14px;
            }
            QPushButton {
                background-color: #3a3f4b;
                color: #f0f0f0;
                border: 1px solid #555;
                border-radius: 6px;
                padding: 6px 16px;
                font-size: 15px;
            }
            QPushButton:hover {
                background-color: #50576a;
            }
            QSplitter::handle {
                background: #444;
            }
        """)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        db_group = QGroupBox("Database Selection")
        db_layout = QVBoxLayout()

        db_type_layout = QHBoxLayout()
        db_type_label = QLabel("Database Type:")
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["postgres", "mongo", "cassandra", "mariadb"])
        self.db_type_combo.setMinimumWidth(180)
        db_type_layout.addWidget(db_type_label)
        db_type_layout.addWidget(self.db_type_combo)
        db_layout.addLayout(db_type_layout)

        test_layout = QHBoxLayout()
        test_label = QLabel("Test Name:")
        self.test_combo = QComboBox()
        filtered_test_names = [name for name in test_names if name not in ["insert", "update", "delete", "select"]]
        self.test_combo.addItems(filtered_test_names)
        self.test_combo.setMinimumWidth(180)
        test_layout.addWidget(test_label)
        test_layout.addWidget(self.test_combo)
        db_layout.addLayout(test_layout)

        records_layout = QHBoxLayout()
        records_label = QLabel("Number of Records:")
        self.records_spin = QSpinBox()
        self.records_spin.setRange(1, 10000)
        self.records_spin.setValue(100)
        self.records_spin.setMinimumWidth(100)
        records_layout.addWidget(records_label)
        records_layout.addWidget(self.records_spin)
        db_layout.addLayout(records_layout)

        executions_layout = QHBoxLayout()
        executions_label = QLabel("Number of Executions:")
        self.executions_spin = QSpinBox()
        self.executions_spin.setRange(1, 100)
        self.executions_spin.setValue(1)
        self.executions_spin.setMinimumWidth(100)
        executions_layout.addWidget(executions_label)
        executions_layout.addWidget(self.executions_spin)
        db_layout.addLayout(executions_layout)

        buttons_layout = QHBoxLayout()
        self.run_button = QPushButton("Run Benchmark")
        self.run_button.setMinimumWidth(140)
        self.run_button.clicked.connect(self.run_benchmark)
        buttons_layout.addWidget(self.run_button)
        db_layout.addLayout(buttons_layout)

        self.exec_time_label = QLabel("Execution Time: -")
        self.num_queries_label = QLabel("Number of Queries: -")
        self.exec_time_label.setStyleSheet("font-size: 13px; color: #b0b0b0;")
        self.num_queries_label.setStyleSheet("font-size: 13px; color: #b0b0b0;")
        db_layout.addWidget(self.exec_time_label)
        db_layout.addWidget(self.num_queries_label)

        db_group.setLayout(db_layout)
        left_layout.addWidget(db_group)
        left_layout.addStretch()

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

        plot_group = QGroupBox("Results Plot")
        plot_layout = QVBoxLayout()
        self.canvas = PlotCanvas(self, width=8, height=5)
        plot_layout.addWidget(self.canvas)
        plot_group.setLayout(plot_layout)
        right_layout.addWidget(plot_group)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([320, 680])

        left_panel.setLayout(left_layout)
        right_panel.setLayout(right_layout)

    def run_benchmark(self):
        db_type = self.db_type_combo.currentText()
        test_name = self.test_combo.currentText()
        records_num = self.records_spin.value()
        executions_num = self.executions_spin.value()

        try:
            credentials = load_database_credentials()
            exec_time = None
            if db_type == "postgres":
                exec_time = run_postgres(credentials, records_num, test_name, executions_num, return_time=True)
            elif db_type == "mongo":
                exec_time = run_mongo(credentials, records_num, test_name, executions_num, return_time=True)
            elif db_type == "cassandra":
                exec_time = run_cassandra(credentials, records_num, test_name, executions_num, return_time=True)
            elif db_type == "mariadb":
                exec_time = run_mariadb(credentials, records_num, test_name, executions_num, return_time=True)

            if exec_time is not None:
                self.exec_time_label.setText(f"Execution Time: {exec_time:.4f} s")
            else:
                self.exec_time_label.setText("Execution Time: -")
            self.num_queries_label.setText(f"Number of Queries: {records_num}")

            QMessageBox.information(self, "Success",
                                    f"Benchmark completed successfully for {db_type} with {records_num} records.")

            self.plot_results()
        except Exception as e:
            self.exec_time_label.setText("Execution Time: -")
            self.num_queries_label.setText("Number of Queries: -")
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")

    def plot_results(self):
        test_name = self.test_combo.currentText()
        success = self.canvas.plot_results(test_name)

        if not success:
            QMessageBox.warning(self, "Warning", f"No results available for test: {test_name}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DatabaseBenchmarkGUI()
    window.show()
    sys.exit(app.exec_())
