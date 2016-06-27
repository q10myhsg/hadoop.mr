package hadoop.matrixs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Cell implements Writable, Comparable<Cell> {
	private int row;
	private int col;
	private int value;
	private String name;

	public Cell() {
	}

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getCol() {
		return col;
	}

	public void setCol(int col) {
		this.col = col;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public int compareTo(Cell o) {
		if (o.row < row)
			return 1;
		else
			return 0;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		row = arg0.readInt();
		col = arg0.readInt();
		value = arg0.readInt();
		name = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(row);
		arg0.writeInt(col);
		arg0.writeInt(value);
		arg0.writeUTF(name);
	}

	@Override
	public String toString() {
		return col + " " + row + " " + value + " " + name;
	}
}
